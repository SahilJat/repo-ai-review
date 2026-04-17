import os
import time
import requests
from typing import Dict, Generator, List, Optional, TypedDict
from loguru import logger

class PRCommentsData(TypedDict):
    """Structured contract for PR comments to ensure downstream stability."""
    formal_reviews: List[Dict]
    inline_comments: List[Dict]

class GithubCrawler:
    def __init__(self, repo_name: str):
        """
        repo_name: e.g., 'Flagsmith/flagsmith'
        """
        self.repo_name = repo_name
        self.base_url = "https://api.github.com"
        
        # Pull the token from your .env file
        self.token = os.getenv("GITHUB_PERSONAL_ACCESS_TOKEN")
        if not self.token:
            raise ValueError("GITHUB_PERSONAL_ACCESS_TOKEN is missing from environment variables.")
        
        # Standard headers for GitHub API
        self.headers = {
            "Accept": "application/vnd.github.v3+json",
            "Authorization": f"Bearer {self.token}",
            "X-GitHub-Api-Version": "2022-11-28"
        }
        
        # Connection pooling for high-throughput sequential requests
        self.session = requests.Session()

    def __enter__(self):
        """Enable context manager support for clean session teardown."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the TCP connection pool when exiting the 'with' block."""
        self.session.close()
        logger.debug("GitHub session closed cleanly.")

    def _get(self, url: str, **kwargs) -> requests.Response:
        """
        Bulletproof wrapper: Connection pooling, timeouts, and multi-tier rate limiting.
        """
        kwargs.setdefault("timeout", 30)
        
        # Fix: Pop headers before the loop so retries don't lose custom headers
        req_headers = kwargs.pop("headers", self.headers)
        
        max_retries = 3
        backoff = 5
        attempt = 0
        
        while attempt < max_retries:
            try:
                response = self.session.get(url, headers=req_headers, **kwargs)
                
                # Secondary Rate Limit (Burst protection - 429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", backoff))
                    logger.warning(f"Secondary rate limit (429) hit. Sleeping {retry_after}s...")
                    time.sleep(retry_after)
                    # Note: We do NOT increment `attempt` here. Rate limits aren't network failures.
                    continue
                    
                # Primary Rate Limit (Hourly quota protection - 403)
                remaining = int(response.headers.get("X-RateLimit-Remaining", 1))
                if remaining < 50:
                    reset_time = int(response.headers.get("X-RateLimit-Reset", 0))
                    sleep_seconds = max(reset_time - time.time() + 5, 60)
                    logger.warning(f"Primary rate limit low ({remaining} left). Sleeping {sleep_seconds:.0f}s...")
                    time.sleep(sleep_seconds)

                response.raise_for_status()
                return response
                
            # Handle both hanging requests and hard network drops
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                attempt += 1
                logger.error(f"Network error on {url} (Attempt {attempt}/{max_retries}): {e}")
                if attempt == max_retries:
                    raise
                time.sleep(backoff)
                backoff *= 2

    def _fetch_paginated_list(self, url: str, params: dict = None) -> list:
        """Helper to exhaust all pages of a list endpoint using response.links."""
        if params is None:
            params = {"per_page": 100}
        else:
            params["per_page"] = 100
            
        all_results = []
        
        while url:
            response = self._get(url, params=params)
            all_results.extend(response.json())
            
            # Cleanly parse the next page using requests native link parsing
            url = response.links.get("next", {}).get("url")
            params = {}  # The 'next' URL already contains all query parameters
            
        return all_results

    def fetch_merged_prs(self, per_page: int = 100) -> Generator[Dict, None, None]:
        """
        Streaming generator that yields ONLY strictly merged PRs.
        Keeps memory flat regardless of repository size.
        """
        url = f"{self.base_url}/repos/{self.repo_name}/pulls"
        params = {
            "state": "closed",
            "per_page": per_page,
            "sort": "updated",
            "direction": "desc"
        }
        
        logger.info(f"Starting paginated fetch of merged PRs for {self.repo_name}...")
        
        while url:
            response = self._get(url, params=params)
            
            for pr in response.json():
                if pr.get("merged_at") is not None:
                    yield pr
                    
            url = response.links.get("next", {}).get("url")
            params = {}

    def fetch_pr_diff(self, pr_number: int) -> Optional[str]:
        """Fetches the raw +/- code changes for RAG context."""
        url = f"{self.base_url}/repos/{self.repo_name}/pulls/{pr_number}"
        
        # Override Accept header strictly for this request
        diff_headers = self.headers.copy()
        diff_headers["Accept"] = "application/vnd.github.v3.diff"
        
        try:
            # We explicitly pass the headers override here
            response = self._get(url, headers=diff_headers)
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch diff for PR #{pr_number}: {e}")
            return None

    def fetch_all_comments(self, pr_number: int) -> PRCommentsData:
        """Fetches all formal reviews and inline comments with strict type adherence."""
        reviews_url = f"{self.base_url}/repos/{self.repo_name}/pulls/{pr_number}/reviews"
        inline_comments_url = f"{self.base_url}/repos/{self.repo_name}/pulls/{pr_number}/comments"
        
        try:
            reviews = self._fetch_paginated_list(reviews_url)
            inline = self._fetch_paginated_list(inline_comments_url)
            
            return PRCommentsData(
                formal_reviews=reviews,
                inline_comments=inline
            )
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch fully paginated comments for PR #{pr_number}: {e}")
            return {"formal_reviews": [], "inline_comments": []}