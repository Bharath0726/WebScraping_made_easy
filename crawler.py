# crawler.py
import os
import asyncio
import requests
from xml.etree import ElementTree
from typing import List, Tuple
from datetime import datetime
from urllib.parse import urlparse
from dotenv import load_dotenv
import re

from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig, CacheMode

load_dotenv()

# Create a directory for markdown files
MARKDOWN_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crawled_content")
os.makedirs(MARKDOWN_DIR, exist_ok=True)


def save_markdown_file(url: str, markdown_content: str):
    """Save the markdown content to a file."""
    # Create a valid filename from the URL
    parsed_url = urlparse(url)
    domain = parsed_url.netloc
    path = parsed_url.path.strip('/')
    
    # Replace invalid filename characters
    filename = re.sub(r'[^\w\-_.]', '_', f"{domain}_{path}")
    
    # Ensure the filename isn't too long
    if len(filename) > 200:
        filename = filename[:200]
    
    # Add timestamp to ensure uniqueness
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename}_{timestamp}.md"
    
    # Save the file
    filepath = os.path.join(MARKDOWN_DIR, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        # Add a header with metadata
        f.write(f"# Crawled Content from {url}\n\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"URL: {url}\n\n")
        f.write("---\n\n")
        f.write(markdown_content)
    
    print(f"Saved markdown file: {filepath}")
    return filepath


def save_sitemap_links_to_markdown(base_url: str, sitemap_urls: List[str]):
    """Save all links from a sitemap to a separate markdown file."""
    # Create a valid filename from the base URL
    parsed_url = urlparse(base_url)
    domain = parsed_url.netloc
    
    # Replace invalid filename characters
    filename = re.sub(r'[^\w\-_.]', '_', f"{domain}_sitemap_links")
    
    # Add timestamp to ensure uniqueness
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename}_{timestamp}.md"
    
    # Save the file
    filepath = os.path.join(MARKDOWN_DIR, filename)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        # Add a header with metadata
        f.write(f"# Sitemap Links from {base_url}\n\n")
        f.write(f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"Base URL: {base_url}\n\n")
        f.write(f"Total URLs found: {len(sitemap_urls)}\n\n")
        f.write("---\n\n")
        f.write("## All URLs in Sitemap\n\n")
        
        # Write each URL as a markdown list item with a link
        for i, url in enumerate(sitemap_urls, 1):
            f.write(f"{i}. [{url}]({url})\n")
    
    print(f"Saved sitemap links to markdown file: {filepath}")
    return filepath


async def process_and_store_document(url: str, markdown: str):
    """Process a document and store it as a markdown file."""
    # Save the markdown file
    save_markdown_file(url, markdown)


def format_sitemap_url(url: str) -> str:
    """Format URL to ensure proper sitemap URL structure."""
    # Remove trailing slashes
    url = url.rstrip("/")

    # Ensure proper URL structure
    if not url.startswith(("http://", "https://")):
        url = f"https://{url}"

    # If URL already contains sitemap reference, don't append sitemap.xml
    if any(x in url.lower() for x in ["sitemap", "sitemap.xml", "sitemap_index.xml"]):
        return url

    # If URL doesn't end with sitemap.xml, append it
    return f"{url}/sitemap.xml"


def get_urls_from_sitemap(sitemap_url: str) -> List[str]:
    """Get URLs from a sitemap or sitemap index."""
    try:
        # Add headers to mimic a browser
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
        }
        
        response = requests.get(sitemap_url, headers=headers)
        response.raise_for_status()

        root = ElementTree.fromstring(response.content)
        
        # Check if this is a sitemap index
        is_sitemap_index = root.tag.endswith('sitemapindex')
        
        # Define namespaces for both sitemap and sitemapindex
        namespace = {"ns": "http://www.sitemaps.org/schemas/sitemap/0.9"}
        
        urls = []
        
        if is_sitemap_index:
            print(f"Found sitemap index at: {sitemap_url}")
            # This is a sitemap index, get all sitemap URLs and process them
            sitemap_urls = [loc.text for loc in root.findall(".//ns:loc", namespace)]
            print(f"Found {len(sitemap_urls)} sitemaps in sitemap index")
            
            # Process each sitemap
            for sub_sitemap_url in sitemap_urls:
                sub_urls = get_urls_from_sitemap(sub_sitemap_url)
                urls.extend(sub_urls)
        else:
            # This is a regular sitemap
            urls = [loc.text for loc in root.findall(".//ns:loc", namespace)]
            print(f"Found {len(urls)} URLs in sitemap: {sitemap_url}")
            
        return urls
    except Exception as e:
        print(f"Error fetching sitemap: {e}")
        return []


def check_and_get_sitemap_urls(url: str) -> Tuple[str, List[str]]:
    """Check if a URL has a sitemap.xml and get all URLs from it."""
    # Format the URL to ensure it has the correct structure
    base_url = url.rstrip("/")
    if not base_url.startswith(("http://", "https://")):
        base_url = f"https://{base_url}"
    
    # Add headers to mimic a browser
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
    }
    
    # Try to get the sitemap URL
    sitemap_url = format_sitemap_url(base_url)
    
    # Get URLs from the sitemap
    urls = get_urls_from_sitemap(sitemap_url)
    
    # If no URLs found, try alternative sitemap locations
    if not urls:
        # Try robots.txt to find sitemap
        try:
            # Extract domain for robots.txt
            parsed_url = urlparse(base_url)
            domain = f"{parsed_url.scheme}://{parsed_url.netloc}"
            
            robots_url = f"{domain}/robots.txt"
            print(f"Checking robots.txt at: {robots_url}")
            response = requests.get(robots_url, headers=headers)
            if response.status_code == 200:
                # Look for Sitemap: directive in robots.txt
                for line in response.text.split('\n'):
                    if line.lower().startswith('sitemap:'):
                        alt_sitemap_url = line.split(':', 1)[1].strip()
                        print(f"Found sitemap reference in robots.txt: {alt_sitemap_url}")
                        urls = get_urls_from_sitemap(alt_sitemap_url)
                        if urls:
                            sitemap_url = alt_sitemap_url
                            break
        except Exception as e:
            print(f"Error checking robots.txt: {e}")
    
    return sitemap_url, urls


async def crawl_parallel(urls: List[str], max_concurrent: int = 5, max_retries: int = 3):
    """Crawl multiple URLs in parallel with a concurrency limit."""
    # Print number of URLs found
    print(f"Found {len(urls)} URLs to crawl")

    # Start timing
    start_time = datetime.now()
    print(f"Crawling started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")

    browser_config = BrowserConfig(
        headless=True,
        verbose=False,
        extra_args=["--disable-gpu", "--disable-dev-shm-usage", "--no-sandbox"],
    )
    crawl_config = CrawlerRunConfig(cache_mode=CacheMode.BYPASS)

    crawler = AsyncWebCrawler(config=browser_config)
    await crawler.start()

    try:
        semaphore = asyncio.Semaphore(max_concurrent)

        # Add a counter for progress tracking
        total_urls = len(urls)
        processed_urls = 0
        failed_urls = 0

        async def process_url(url: str):
            nonlocal processed_urls, failed_urls
            async with semaphore:
                for retry in range(max_retries):
                    try:
                        result = await crawler.arun(
                            url=url, config=crawl_config, session_id="session1"
                        )
                        if result.success:
                            processed_urls += 1
                            # Calculate progress percentage
                            progress_pct = (processed_urls / total_urls) * 100
                            # Calculate elapsed time
                            elapsed_time = datetime.now() - start_time
                            # Estimate remaining time
                            if processed_urls > 0:
                                avg_time_per_url = elapsed_time / processed_urls
                                remaining_urls = total_urls - processed_urls
                                est_remaining_time = avg_time_per_url * remaining_urls
                                print(
                                    f"Successfully crawled: {url} ({processed_urls}/{total_urls}, {progress_pct:.1f}%) - "
                                    f"Elapsed: {str(elapsed_time).split('.')[0]}, Est. remaining: {str(est_remaining_time).split('.')[0]}"
                                )
                            else:
                                print(
                                    f"Successfully crawled: {url} ({processed_urls}/{total_urls}, {progress_pct:.1f}%) - "
                                    f"Elapsed: {str(elapsed_time).split('.')[0]}"
                                )
                            await process_and_store_document(
                                url, result.markdown_v2.raw_markdown
                            )
                            return
                        else:
                            print(f"Failed: {url} - Error: {result.error_message}")
                            if retry < max_retries - 1:
                                print(f"Retrying ({retry + 1}/{max_retries})...")
                                await asyncio.sleep(2)  # Wait before retrying
                            else:
                                failed_urls += 1
                                print(f"Max retries reached for {url}")
                    except Exception as e:
                        print(f"Exception while crawling {url}: {str(e)}")
                        if retry < max_retries - 1:
                            print(f"Retrying ({retry + 1}/{max_retries})...")
                            await asyncio.sleep(2)  # Wait before retrying
                        else:
                            failed_urls += 1
                            print(f"Max retries reached for {url}")

        await asyncio.gather(*[process_url(url) for url in urls])
        
        # Calculate total time
        end_time = datetime.now()
        total_time = end_time - start_time
        
        print(f"Completed crawling {processed_urls} out of {total_urls} URLs")
        print(f"Failed to crawl {failed_urls} URLs")
        print(f"Crawling started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Crawling finished at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Total crawling time: {str(total_time).split('.')[0]}")
        
        # Calculate statistics
        if processed_urls > 0:
            avg_time_per_url = total_time / processed_urls
            print(f"Average time per URL: {str(avg_time_per_url).split('.')[0]}")
            urls_per_minute = (processed_urls / total_time.total_seconds()) * 60
            print(f"URLs crawled per minute: {urls_per_minute:.2f}")
        
        # Return timing information
        return {
            "start_time": start_time,
            "end_time": end_time,
            "total_time": total_time,
            "processed_urls": processed_urls,
            "failed_urls": failed_urls
        }
    finally:
        await crawler.close()


async def main():
    """Main function for standalone execution."""
    print("Running crawler in standalone mode...")
    print(f"Markdown files will be saved to: {MARKDOWN_DIR}")
    
    # Get the URL from command line arguments or use a default
    import sys
    if len(sys.argv) > 1:
        base_url = sys.argv[1]
    else:
        base_url = "your_website_url"
    
    # Create a log directory
    LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "crawl_logs")
    os.makedirs(LOG_DIR, exist_ok=True)
    
    # Create a log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_filename = f"crawl_log_{timestamp}.txt"
    log_filepath = os.path.join(LOG_DIR, log_filename)
    
    # Start timing the entire process
    process_start_time = datetime.now()
    
    print(f"Processing URL: {base_url}")
    print(f"Log will be saved to: {log_filepath}")
    
    # Open log file
    with open(log_filepath, 'w', encoding='utf-8') as log_file:
        # Write header
        log_file.write(f"Crawl Log for {base_url}\n")
        log_file.write(f"Started at: {process_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        
        # Check for sitemap and get URLs
        sitemap_check_start = datetime.now()
        sitemap_url, sitemap_urls = check_and_get_sitemap_urls(base_url)
        sitemap_check_time = datetime.now() - sitemap_check_start
        
        log_file.write(f"Sitemap check completed in: {str(sitemap_check_time).split('.')[0]}\n")
        
        if sitemap_urls:
            log_file.write(f"Found {len(sitemap_urls)} URLs in sitemap: {sitemap_url}\n\n")
            print(f"Found {len(sitemap_urls)} URLs in sitemap: {sitemap_url}")
            
            # Save all sitemap links to a markdown file
            sitemap_links_file = save_sitemap_links_to_markdown(base_url, sitemap_urls)
            log_file.write(f"Saved sitemap links to: {sitemap_links_file}\n\n")
            
            # Ask if user wants to crawl all URLs
            crawl_all = input(f"Do you want to crawl all {len(sitemap_urls)} URLs? (y/n): ")
            log_file.write(f"User chose to crawl all URLs: {crawl_all.lower() == 'y'}\n")
            
            crawl_stats = None
            if crawl_all.lower() == 'y':
                # Crawl all URLs from sitemap
                log_file.write(f"Crawling all {len(sitemap_urls)} URLs...\n")
                crawl_stats = await crawl_parallel(sitemap_urls)
            else:
                # Ask how many URLs to crawl
                try:
                    num_urls = int(input(f"How many URLs do you want to crawl (1-{len(sitemap_urls)})? "))
                    num_urls = max(1, min(num_urls, len(sitemap_urls)))
                    log_file.write(f"Crawling {num_urls} URLs...\n")
                    print(f"Crawling {num_urls} URLs...")
                    crawl_stats = await crawl_parallel(sitemap_urls[:num_urls])
                except ValueError:
                    log_file.write("Invalid input. Crawling just the base URL.\n")
                    print("Invalid input. Crawling just the base URL.")
                    crawl_stats = await crawl_parallel([base_url])
        else:
            log_file.write(f"No sitemap found or empty sitemap for {base_url}. Crawling the base URL only.\n")
            print(f"No sitemap found or empty sitemap for {base_url}. Crawling the base URL only.")
            crawl_stats = await crawl_parallel([base_url])
        
        # Calculate total process time
        process_end_time = datetime.now()
        total_process_time = process_end_time - process_start_time
        
        # Write crawl statistics to log
        if crawl_stats:
            log_file.write("\n--- Crawl Statistics ---\n")
            log_file.write(f"Crawling started at: {crawl_stats['start_time'].strftime('%Y-%m-%d %H:%M:%S')}\n")
            log_file.write(f"Crawling finished at: {crawl_stats['end_time'].strftime('%Y-%m-%d %H:%M:%S')}\n")
            log_file.write(f"Total crawling time: {str(crawl_stats['total_time']).split('.')[0]}\n")
            log_file.write(f"Successfully crawled URLs: {crawl_stats['processed_urls']}\n")
            log_file.write(f"Failed URLs: {crawl_stats['failed_urls']}\n")
            
            if crawl_stats['processed_urls'] > 0:
                avg_time = crawl_stats['total_time'] / crawl_stats['processed_urls']
                urls_per_minute = (crawl_stats['processed_urls'] / crawl_stats['total_time'].total_seconds()) * 60
                log_file.write(f"Average time per URL: {str(avg_time).split('.')[0]}\n")
                log_file.write(f"URLs crawled per minute: {urls_per_minute:.2f}\n")
        
        # Write total process time
        log_file.write("\n--- Total Process Statistics ---\n")
        log_file.write(f"Process started at: {process_start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Process finished at: {process_end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
        log_file.write(f"Total process time: {str(total_process_time).split('.')[0]}\n")
    
    print(f"Crawling complete. Markdown files saved to: {MARKDOWN_DIR}")
    print(f"Crawl log saved to: {log_filepath}")
    print(f"Total process time: {str(total_process_time).split('.')[0]}")


if __name__ == "__main__":
    asyncio.run(main())
