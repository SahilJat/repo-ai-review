import os
import time
import requests
from typing import List, Dict, Optional, Generator
from loguru import logger

class GithubCrawler:
    def __init__(self, repo_name: str):
        """
        repo_name: e.g., 'YourUsername/flagsmith'
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
        # Connection pooling for faster sequential requests
        self.session = requests.Session()

    def _get(self, url: str, **kwargs) -> requests.Response:
        """
        Bulletproof wrapper: Connection pooling, timeouts, and multi-tier rate limiting.
        """
        kwargs.setdefault("timeout", 30)
        max_retries = 3
        backoff = 5
        
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, headers=self.headers, **kwargs)
                
                # Secondary Rate Limit (Burst protection - 429)
                if response.status_code == 429:
                    retry_after = int(response.headers.get("Retry-After", backoff))
                    logger.warning(f"Secondary rate limit (429) hit. Sleeping {retry_after}s...")
                    time.sleep(retry_after)
                    backoff *= 2  # Exponential backoff
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
                
            except requests.exceptions.Timeout:
                logger.error(f"Timeout on {url} (Attempt {attempt + 1}/{max_retries})")
                if attempt == max_retries - 1:
                    raise
                time.sleep(backoff)
                backoff *= 2

    def fetch_merged_prs(self, per_page: int = 100) -> Generator[Dict, None, None]:
        """
        Yields ONLY merged PRs. Handles pagination via the 'Link' header.
        Using a generator keeps memory usage flat regardless of repo size.
        """
        url = f"{self.base_url}/repos/{self.repo_name}/pulls"
        params = {
            "state": "closed",  # Only closed PRs have a chance of being merged
            "per_page": per_page,
            "sort": "updated",
            "direction": "desc"
        }
        
        logger.info(f"Starting paginated fetch of merged PRs for {self.repo_name}...")
        
        while url:
            response = self._get(url, params=params)
            prs = response.json()
            
            # Filter and yield ground-truth merged code
            for pr in prs:
                if pr.get('merged_at'):
                    yield pr
            
            # Handle Pagination safely
            link_header = response.headers.get('Link', '')
            url = None  # Default to stopping
            
            if 'rel="next"' in link_header:
                links = link_header.split(', ')
                for link in links:
                    if 'rel="next"' in link:
                        # Extract the exact URL between the < > brackets
                        url = link[link.find('<')+1:link.find('>')]
                        params = None  # Next URL already contains the params

    def fetch_pr_diff(self, pr_number: int) -> Optional[str]:
        """Fetches the raw +/- code changes."""
        url = f"{self.base_url}/repos/{self.repo_name}/pulls/{pr_number}"
        
        #change the Accept header specifically to get the raw diff format
        diff_headers = self.headers.copy()
        diff_headers["Accept"] = "application/vnd.github.v3.diff"
        
        try:
            response = self._get(url, headers=diff_headers)
            return response.text
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch diff for PR #{pr_number}: {e}")
            return None

    def fetch_all_comments(self, pr_number: int) -> Dict[str, List[Dict]]:
        """
        Fetches both formal review states AND the crucial inline code comments.
        """
        reviews_url = f"{self.base_url}/repos/{self.repo_name}/pulls/{pr_number}/reviews"
        inline_comments_url = f"{self.base_url}/repos/{self.repo_name}/pulls/{pr_number}/comments"
        
        try:
            reviews = self._get(reviews_url).json()
            inline = self._get(inline_comments_url).json()
            
            return {
                "formal_reviews": reviews,
                "inline_comments": inline
            }
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch comments for PR #{pr_number}: {e}")
            return {"formal_reviews": [], "inline_comments": []}