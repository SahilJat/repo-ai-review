import os
import uuid
import psycopg2
from loguru import logger
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
from sentence_transformers import SentenceTransformer

class PRDataPipeline:
    def __init__(self):
        # 1. Setup PostgreSQL (Checkpointing)
        db_user = os.getenv("POSTGRES_USER", "postgres")
        db_pass = os.getenv("POSTGRES_PASSWORD", "postgres")
        db_name = os.getenv("POSTGRES_DB", "postgres")
        
        self.pg_conn = psycopg2.connect(
            host="localhost", port=5432, 
            user=db_user, password=db_pass, dbname=db_name
        )
        self.pg_cursor = self.pg_conn.cursor()
        self._init_postgres()

        # 2. Setup Qdrant (Vector Storage)
        self.qdrant = QdrantClient(url="http://localhost:6333", timeout=60.0)
        self.collection_name = "flagsmith_codebase"

        # 3. Setup HuggingFace Embeddings
       
        logger.info("Loading BAAI/bge-base-en-v1.5 model...")
        self.model = SentenceTransformer('BAAI/bge-base-en-v1.5')
        self.vector_size = 768  # bge-base-en-v1.5 uses 768 dimensions
        
        self._init_qdrant()

    def __enter__(self):
        """Enable context manager support for safe DB teardown."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Guarantee DB connections close, even on pipeline crashes."""
        if hasattr(self, 'pg_cursor'): 
            self.pg_cursor.close()
        if hasattr(self, 'pg_conn'): 
            self.pg_conn.close()
        logger.debug("PostgreSQL connection closed cleanly.")

    def _init_postgres(self):
        """Creates the state table to track which PRs have been ingested."""
        self.pg_cursor.execute("""
            CREATE TABLE IF NOT EXISTS ingestion_state (
                pr_number INTEGER PRIMARY KEY,
                status VARCHAR(50),
                chunk_count INTEGER,
                ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.pg_conn.commit()

    def _init_qdrant(self):
        """Creates the Qdrant collection if it doesn't exist."""
        collections = self.qdrant.get_collections().collections
        if not any(c.name == self.collection_name for c in collections):
            logger.info(f"Creating Qdrant collection: {self.collection_name}")
            self.qdrant.create_collection(
                collection_name=self.collection_name,
                vectors_config=VectorParams(size=self.vector_size, distance=Distance.COSINE)
            )

    def is_pr_processed(self, pr_number: int) -> bool:
        """Checks Postgres to see if we already ingested this PR."""
        self.pg_cursor.execute(
            "SELECT status FROM ingestion_state WHERE pr_number = %s", 
            (pr_number,)
        )
        result = self.pg_cursor.fetchone()
        return result is not None and result[0] == 'COMPLETED'

    def mark_pr_processed(self, pr_number: int, chunk_count: int):
        """Idempotent checkpoint save using ON CONFLICT DO UPDATE."""
        self.pg_cursor.execute("""
            INSERT INTO ingestion_state (pr_number, status, chunk_count) 
            VALUES (%s, 'COMPLETED', %s)
            ON CONFLICT (pr_number) 
            DO UPDATE SET status = 'COMPLETED', chunk_count = %s, ingested_at = CURRENT_TIMESTAMP
        """, (pr_number, chunk_count, chunk_count))
        self.pg_conn.commit()

    def _format_comments(self, comments_data: dict) -> str:
        """
        TRANSFORM: Extracts pure semantic signal from comments.
        Drops JSON metadata so vectors aren't poisoned by UUIDs and URLs.
        """
        parts = []
        for review in comments_data.get("formal_reviews", []):
            if review.get("body", "").strip():
                parts.append(f"Review by {review['user']['login']}: {review['body']}")
                
        for comment in comments_data.get("inline_comments", []):
            if comment.get("body", "").strip():
                path = comment.get("path", "unknown file")
                parts.append(f"Inline comment on {path}: {comment['body']}")
                
        return "\n".join(parts)

    def clean_and_chunk(self, raw_text: str, chunk_size: int = 1500, overlap: int = 200) -> list[str]:
        """Cleans junk data and splits massive diffs into overlapping windows."""
        if not raw_text:
            return []
            
        cleaned_lines = []
        skip = False
        for line in raw_text.split('\n'):
            if line.startswith('diff --git'):
                skip = any(ext in line for ext in ['.lock', '.png', '.svg', '.min.js'])
            if not skip and line.strip():
                cleaned_lines.append(line.strip())
                
        clean_text = '\n'.join(cleaned_lines)
        
        chunks = []
        start = 0
        while start < len(clean_text):
            end = start + chunk_size
            chunks.append(clean_text[start:end])
            start += (chunk_size - overlap)
        return chunks

    def process_pr(self, pr_number: int, title: str, diff_text: str, comments_data: dict):
        """The main ETL runner for a single PR."""
        if self.is_pr_processed(pr_number):
            logger.info(f"⏭️  Skipping PR #{pr_number} (Already in database)")
            return

        logger.info(f"⚙️  Processing PR #{pr_number}...")
        
        formatted_comments = self._format_comments(comments_data)
        
        combined_text = f"PR Title: {title}\n\n"
        if formatted_comments:
            combined_text += "Maintainer Reviews and Comments:\n" + formatted_comments + "\n\n"
        if diff_text:
            combined_text += "Code Diffs:\n" + diff_text
            
        chunks = self.clean_and_chunk(combined_text)
        if not chunks:
            logger.warning(f"⚠️  No valid text chunks found for PR #{pr_number}")
            self.mark_pr_processed(pr_number, 0)
            return

        # Batched encoding: Submits all chunks to the model in a single forward pass
        vectors = self.model.encode(chunks, batch_size=32, show_progress_bar=False)
        
        # Zip the chunks and batched vectors together to create Qdrant payload
        points = [
            PointStruct(
                id=str(uuid.uuid4()),
                vector=vec.tolist(),
                payload={
                    "pr_number": pr_number,
                    "title": title,
                    "chunk_index": i,
                    "text": chunk #  We must store the raw text to feed the LLM during retrieval
                }
            )
            for i, (chunk, vec) in enumerate(zip(chunks, vectors))
        ]
            
        self.qdrant.upsert(collection_name=self.collection_name, points=points)
        self.mark_pr_processed(pr_number, len(chunks))
        
        logger.success(f"✅ Embedded and stored {len(chunks)} chunks for PR #{pr_number}")