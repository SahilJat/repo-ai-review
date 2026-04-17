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

    def _get(self, url: str, headers: dict = None, params: dict = None) -> requests.Response:
        """
        Wrapper to handle rate limiting seamlessly. Pauses execution if the limit is hit
        and resumes precisely when GitHub resets the quota.
        """
        req_headers = headers if headers else self.headers
        
        while True:
            response = self.session.get(url, headers=req_headers, params=params)
            
            # Check for Rate Limit Exception (403 Forbidden with specific headers)
            if response.status_code == 403 and 'X-RateLimit-Remaining' in response.headers:
                if int(response.headers.get('X-RateLimit-Remaining', 1)) == 0:
                    reset_time = int(response.headers.get('X-RateLimit-Reset', time.time() + 60))
                    sleep_time = max(reset_time - int(time.time()), 0) + 5  # 5-second buffer
                    
                    logger.warning(f"GitHub rate limit exhausted. Sleeping for {sleep_time} seconds...")
                    time.sleep(sleep_time)
                    continue  # Retry the exact same request once awake
            
            response.raise_for_status()
            return response

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