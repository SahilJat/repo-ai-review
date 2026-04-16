import os
import requests
from typing import List, Dict, Optional
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

    def fetch_prs(self, state: str = "all", per_page: int = 30) -> List[Dict]:
        """
        Fetches Pull Requests. 
        State can be 'open', 'closed', or 'all'.
        """
        url = f"{self.base_url}/repos/{self.repo_name}/pulls"
        params = {
            "state": state,
            "per_page": per_page,
            "sort": "updated",
            "direction": "desc"
        }
        
        logger.info(f"Fetching {state} PRs for {self.repo_name}...")
        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()
        
        return response.json()

    def fetch_pr_diff(self, pr_number: int) -> Optional[str]:
        """
        This is the most critical method for RAG!
        It fetches the actual code changes (+ and - lines) so the LLM can read the syntax.
        """
        url = f"{self.base_url}/repos/{self.repo_name}/pulls/{pr_number}"
        
        # We change the Accept header specifically to get the raw diff format
        diff_headers = self.headers.copy()
        diff_headers["Accept"] = "application/vnd.github.v3.diff"
        
        response = requests.get(url, headers=diff_headers)
        
        if response.status_code == 200:
            return response.text
        else:
            logger.error(f"Failed to fetch diff for PR #{pr_number}: {response.status_code}")
            return None

    def fetch_pr_comments(self, pr_number: int) -> List[Dict]:
        """
        Fetches the review comments on the PR to learn maintainer behavior.
        """
        url = f"{self.base_url}/repos/{self.repo_name}/pulls/{pr_number}/reviews"
        
        response = requests.get(url, headers=self.headers)
        response.raise_for_status()
        
        return response.json()