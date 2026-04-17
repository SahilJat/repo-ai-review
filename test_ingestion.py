import os
from dotenv import load_dotenv
from ingestion.github_client import GithubCrawler

load_dotenv()

def run_test():
    target_prs = 10
    print(f"Initializing GithubCrawler test for the last {target_prs} merged PRs...\n")
    
    with GithubCrawler(repo_name="Flagsmith/flagsmith") as crawler:
        # We fetch 20 per page just to be safe, but we will break the loop at 10
        pr_stream = crawler.fetch_merged_prs(per_page=20)
        
        prs_tested = 0
        total_formal = 0
        total_inline = 0
        
        for pr in pr_stream:
            if prs_tested >= target_prs:
                break
                
            pr_number = pr['number']
            title = pr['title']
            print(f"🔍 Checking PR #{pr_number}: {title}")
            
            # Fetch the comments using our hardened pagination wrapper
            comments_data = crawler.fetch_all_comments(pr_number)
            
            formal_count = len(comments_data.get('formal_reviews', []))
            inline_count = len(comments_data.get('inline_comments', []))
            
            total_formal += formal_count
            total_inline += inline_count
            
            print(f"   ↳ Formal Reviews: {formal_count}")
            print(f"   ↳ Inline Comments: {inline_count}")
            
            # If we found an inline comment, let's peek at the first 100 characters of the first one
            if inline_count > 0:
                first_comment_body = comments_data['inline_comments'][0].get('body', '')
                # Clean up newlines for a prettier terminal output
                preview = first_comment_body.replace('\n', ' ')[:100]
                print(f"   💬 Preview: \"{preview}...\"")
                
            print("") # Spacing between PRs
            prs_tested += 1

        print("--- Test Complete ---")
        print(f"Total Formal Reviews found: {total_formal}")
        print(f"Total Inline Comments found: {total_inline}")

if __name__ == "__main__":
    run_test()