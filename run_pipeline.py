import os
from dotenv import load_dotenv
from ingestion.github_client import GithubCrawler
from ingestion.etl import PRDataPipeline

load_dotenv()

def run():
    print("🚀 Starting Local ETL Pipeline...")
    
    
    pipeline = PRDataPipeline()
    
    # We will process the last 10 merged PRs
    target_prs = 10
    prs_processed = 0
    
    with GithubCrawler(repo_name="Flagsmith/flagsmith") as crawler:
        pr_stream = crawler.fetch_merged_prs(per_page=20)
        
        for pr in pr_stream:
            if prs_processed >= target_prs:
                break
                
            pr_number = pr['number']
            title = pr['title']
            
            # 1. CHECKPOINT: Did we already do this one?
            if pipeline.is_pr_processed(pr_number):
                print(f"⏭️  Skipping PR #{pr_number} (Already in Postgres)")
                continue
                
            print(f"\n📥 Fetching PR #{pr_number}: {title}")
            
            # 2. EXTRACT: Get the raw context
            diff_text = crawler.fetch_pr_diff(pr_number)
            comments_data = crawler.fetch_all_comments(pr_number)
            
            # 3. TRANSFORM & LOAD: Clean, chunk, embed, and store in Qdrant + Postgres
            pipeline.process_pr(pr_number, title, diff_text, comments_data)
            
            prs_processed += 1

    print("\n🎉 ETL Pipeline Finished!")

if __name__ == "__main__":
    run()