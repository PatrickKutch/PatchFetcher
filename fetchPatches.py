##############################################################################
#  Copyright (c) 2024 Intel Corporation
#
# Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
##############################################################################
#    File Abstract:
#        Reads patches from a public inbox, placing them in a specified directory
#        for whatever later processing might be desired
#
#    Author: Patrick Kutch
##############################################################################
import os
import subprocess
import requests
import time
from bs4 import BeautifulSoup
from rich.console import Console
from rich.table import Table
from rich.progress import track
from rich.progress import Progress
from datetime import datetime
import json
import shutil
import gzip
from concurrent.futures import ThreadPoolExecutor
import re
import random




__author__      = "Patrick Kutch"
__autor_email__ = "Patrick.Kutch@Gmail.com"
__version__     = "24.12.30"

b4_retry_count = 4 # add one
b4_retry_interval = 10
b4_thread_count = 10
read_http_sleep = 10

def json_file_to_list(filename: str):
    """Read a dictionary from a json file."""
    if filename:
        try:
            print(f"Opening file: {filename}")
            with open(filename, "rt") as fp:
                return json.load(fp)

        except Exception:
            pass
            #print(f"{filename} was invalid or empty.")
    
    return []


def data_to_json_file(filename, dict_to_write):
    """Write a dictionary (hash table) to a json file."""
    #print(f"Writing to {filename}.")
    with open(filename, "wt") as fp:
        fp.write(json.dumps(dict_to_write))

       
def download_mbx_thread(thread_url, base_url, thread_title, output_dir, max_retries=5):
    full_url = f"{base_url}{thread_url.lstrip('/')}"
    download_url = full_url.replace("/T/", "/t.mbox.gz")
    thread_id = thread_url.rstrip('/').split("/")[-2]

    orig_title = thread_title
    if len(thread_title) > 200:
        thread_title = thread_title[:200].rsplit(' ', 1)[0]

    sanitized_title = re.sub(r'[^a-zA-Z0-9-_]', '_', thread_title).strip('_')
    if sanitized_title.upper() == "UNKNOWN":
        # when folks send a general email to the email distro,
        # the title becomes (unknown), so let's skip it
        return "skipped"

    thread_dir = os.path.abspath(os.path.join(output_dir, sanitized_title))

    os.makedirs(thread_dir, exist_ok=True)

    mbx_gz_file_path = os.path.join(thread_dir, f"{thread_id}.mbx.gz")
    mbx_file_path = os.path.join(thread_dir, f"{thread_id}.mbx")

    # Check for the existence of any .mbx file in the thread_dir directory
    if any(fname.endswith('.mbx') for fname in os.listdir(thread_dir)):
        return thread_dir

    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'}

    for attempt in range(max_retries + 1):
        try:
            error_log_path = os.path.join(thread_dir, "error.txt")
            # remove the error.txt file - this could be a retry
            try:
                os.remove(error_log_path)
            except FileNotFoundError:
                pass

            response = requests.get(download_url, stream=True, headers=headers, timeout=(5, 30))
            response.raise_for_status()

            # read the .mbx compressed file and write it to local storage
            with open(mbx_gz_file_path, "wb") as mbx_gz_file:
                mbx_gz_file.write(response.content)

            # uncompress the file
            with gzip.open(mbx_gz_file_path, "rb") as gz_file, open(mbx_file_path, "wb") as mbx_file:
                shutil.copyfileobj(gz_file, mbx_file)

            # remove the compressed file
            os.remove(mbx_gz_file_path)

            with open(os.path.join(thread_dir, "download_info.txt"), "w") as meta_file:
                meta_file.write(f"Download URL: {download_url}\nSaved as: {mbx_file_path}\n")
                if len(orig_title) > 200:
                    meta_file.write(f"Thread title was truncated:\n{orig_title}\n")

            return thread_dir

        except requests.exceptions.RequestException as req_err:
            if response.status_code == 503 and attempt < max_retries:
                wait_time = (2 ** attempt) + random.uniform(0, 1)  # Exponential backoff with jitter
                #print(f"503 Service Unavailable. Retrying in {wait_time:.2f} seconds... (Attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue

            print(f"{str(req_err)} downloading .mbx : {thread_dir} -> {download_url}")

            with open(error_log_path, "w") as error_file:
                error_file.write(f"HTTP Error: {str(req_err)}\nURL: {download_url}\n")
            return None

        except OSError as os_err:
            error_log_path = os.path.join(thread_dir, "error.txt")
            with open(error_log_path, "w") as error_file:
                error_file.write(f"File Error: {str(os_err)}\n")
            return None

        except Exception as req_err:
            error_log_path = os.path.join(thread_dir, "error.txt")
            with open(error_log_path, "w") as error_file:
                error_file.write(f"Error: {str(req_err)}\nURL: {download_url}\n")
            return None

def get_page(url):
    """Fetch and parse an HTML page using BeautifulSoup."""
    response = requests.get(url)
    response.raise_for_status()
    return BeautifulSoup(response.text, "html.parser")

def extract_topic_threads(soup):
    """Extract topic thread URLs and titles from the page soup."""
    topic_threads = []
    for link in soup.find_all('a', href=True):
        href = link['href']
        if href.endswith('/T/#t'):
            url = href.split('#')[0]
            title = link.text.strip()
            topic_threads.append((url, title))
    return topic_threads

def fetch_all_threads(base_url, start_date, end_date,cacheFileName):
    """Fetch all threads from a base URL until the specified oldest year."""
    if start_date:
        start_date = datetime.strptime(start_date, "%Y-%m-%d")
        start_date_str = start_date.strftime("%Y%m%d%H%M%S")
        next_page = base_url.rstrip('/') + '/?t=' + start_date_str
    else:
        start_date = datetime.now()  # only used for progress bar
        next_page = base_url
        
    first_page = next_page
    
    oldest_date = datetime.strptime(end_date, "%Y-%m-%d")
    total_time_range = (start_date - oldest_date).total_seconds()

    thread_data = []  # Use a list instead of a set
    cutoff_date = oldest_date
    cache = json_file_to_list(cacheFileName)
    newestCachedPage = 0
    oldestCachedPage = 99999999999999
    cachedTopics = {}

    if cache:
        checkForCachedData = True
        for page, topic_threads in cache:  # Only process topic_threads from the cache
            for thread_info in topic_threads:
                if thread_info[0] not in cachedTopics:
                    cachedTopics[thread_info[0]] = 1
                    thread_data.append(thread_info)
                else:
                    pass  # ignore duplicates - they will be older

            timestamp = page.split("t=")[-1]
            try:
                page_date = int(timestamp)
                if page_date > newestCachedPage:
                    newestCachedPage = page_date
                if page_date < oldestCachedPage:
                    oldestCachedPage = page_date
            except ValueError:
                continue
    else:
        checkForCachedData = False

    # Add progress tracking
    with Progress() as progress:
        progress_task = progress.add_task(f"[cyan]Fetching threads from {base_url}..", total=100)

        while next_page:
            #console.print(f"[bold blue]Fetching page:[/bold blue] {next_page}")

            try:
                soup = get_page(next_page)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503:
                    console.print(f"[yellow]503 Service Unavailable: Retrying after delay...[/yellow]")
                    time.sleep(read_http_sleep)  # Wait for nn seconds before retrying
                    continue
            except Exception as e:
                console.print(f"[red]Error fetching page: {next_page}[/red]")
                console.print(f"[red]{e}[/red]")
                break

            topic_threads = extract_topic_threads(soup)
            #console.print(f"[green]Found {len(topic_threads)} topic threads on page.[/green]")
            cacheFileNeedsUpdate = False
            for thread_info in topic_threads:
                # keep track of all topics
                if thread_info[0] not in cachedTopics:
                    cachedTopics[thread_info[0]] = 1
                    thread_data.append(thread_info)
                    cacheFileNeedsUpdate = True
                else:
                    pass  # ignore duplicates - they will be older
            
            if cacheFileName and cacheFileNeedsUpdate and not next_page == base_url:
                # don't cache it if it is the base URL, so we can always get the latest
                # if don't specify a start
                cache.append((next_page, topic_threads))  
                data_to_json_file(cacheFileName, cache)

            next_link = soup.select_one('a[rel="next"]')
            if next_link:
                next_page = next_link['href']
                if not next_page.startswith("http"):
                    next_page = base_url.rstrip('/') + '/' + next_page.lstrip('/')
                
                # Check the date from the 'next_link'
                if "t=" in next_page:
                    timestamp = next_page.split("t=")[-1]
                    try:
                        page_date = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
                        if checkForCachedData and int(timestamp) < newestCachedPage:
                            # so the timestamp on t= is older than the youngest cached page
                            # so lets just skip to the oldest cached page, and continue
                            checkForCachedData = False
                            oldest_timestamp = str(oldestCachedPage)
                            page_date = datetime.strptime(oldest_timestamp, "%Y%m%d%H%M%S")
                            next_page = next_page.split("t=")[0] + "t=" + oldest_timestamp
                        
                        # Update progress
                        elapsed_time = (start_date - page_date).total_seconds()
                        progress_percentage = min(100, max(0, (elapsed_time / total_time_range) * 100))
                        progress.update(progress_task, completed=progress_percentage)

                        if page_date < cutoff_date:
                            console.print(f"[yellow]Reached start date: {page_date}[/yellow]")
                            return thread_data
                    
                    except ValueError:
                        console.print(f"[red]Invalid timestamp in next_link: {timestamp}[/red]")
                        break
            else:
                next_page = None

        console.print(f"[green]Found {len(thread_data)} threads to process.[/green]")
        return thread_data


def fetch_and_parse_threads(base_url, start_date, oldest_date, output_dir,cacheFileName):
    """Fetch and parse all threads from the base URL until the specified oldest year."""
   # Convert date strings to datetime objects
    if start_date:
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
    else:
        start_date_obj = datetime.now()  # Default to current date

    oldest_date_obj = datetime.strptime(oldest_date, "%Y-%m-%d")

    # Validate that start_date is older than oldest_date
    if start_date_obj <= oldest_date_obj:
        raise ValueError(f"start_date ({start_date}) must be newer than oldest_date ({oldest_date}).")
    
    
    os.makedirs(output_dir, exist_ok=True)
    thread_data = fetch_all_threads(base_url, start_date, oldest_date,cacheFileName)
    all_threads = []

    def process_thread(thread):
        thread_url, thread_title, base_url = thread
        #return fetch_thread_with_b4(thread_url, base_url, thread_title, output_dir)
        retVal = download_mbx_thread(thread_url, base_url, thread_title, output_dir)
        
        return retVal, thread
    
    total_threads = len(thread_data)
    processed_threads = 0
    print(f"Writing files to {output_dir}")
    
    threadsWithErrors = []
    
    with Progress() as progress:
        task = progress.add_task(f"[blue]Fetching MBX files for {total_threads} threads...", total=total_threads)

        # this can be run with multiple threads, and it is a bit faster
        # however a lot of 503 error will occur (depending on the time of day it seems)
        # so if we run single thread is slower, but does not seem to get the errors
        if False:
            workers = b4_thread_count
            with ThreadPoolExecutor(max_workers=2) as executor:
                for thread_dir, threadInfo in executor.map(process_thread, ((thread[0], thread[1], base_url) for thread in thread_data)):
                    processed_threads += 1
                    progress.update(task, advance=1)  # Increment the progress bar
                    if not thread_dir:
                        threadsWithErrors.append(threadInfo)

        else:                    
            for thread in thread_data:
                thread_dir, threadInfo = process_thread((thread[0], thread[1], base_url))
                processed_threads += 1
                progress.update(task, advance=1)  # Increment the progress bar
                if not thread_dir:
                    threadsWithErrors.append(threadInfo)
                    
                    
    # even with the retries and timeouts, still get a lot of 503 errors
    # especially during the day, so go through, with a single thread this time
    # and try a few more times.
    if threadsWithErrors:
        repeatCount=3
        withErrors=[]
        while threadsWithErrors and repeatCount:
            print(f"{len(threadsWithErrors)} errors found while fetching {total_threads} .MBX files.  Trying once again.")
            for threadInfo in threadsWithErrors:
                if not process_thread(threadInfo):
                    withErrors.append(threadInfo)
                    
            threadsWithErrors = withErrors    
            repeatCount -= 1


def display_threads(threads):
    """Display threads using rich."""
    for thread in threads:
        console.print(f"[bold green]Thread URL:[/bold green] {thread['url']}")

        table = Table(title="Thread Details")
        table.add_column("Content", style="cyan")

        for email in thread["emails"]:
            table.add_row(email["content"][:30] + "..." if len(email["content"]) > 30 else email["content"])

        console.print(table)

if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser(description=f"Fetch and locally threads from lore.kernel.org using b4. v{__version__}")
    subparsers = parser.add_subparsers(dest="mode", required=True)

    fetch_parser = subparsers.add_parser("fetch-patches", help="Fetch patches from the specified base URL.")
    fetch_parser.add_argument(
        "--base-url",
        required=True,
        help="Base URL to fetch threads from, e.g., https://lore.kernel.org/netdev/",
    )
    fetch_parser.add_argument(
        "--start-date",
        required=False,
        help="Start from a specific date. e.g.,  e.g., 2024-12-01. Default is to start from now",
    )
    fetch_parser.add_argument(
        "--end-date",
        required=True,
        help="Oldest date to fetch threads for, e.g., 2024-12-02.",
    )
    fetch_parser.add_argument(
        "--output-dir",
        default="b4_threads",
        help="Directory to save fetched mbx files.",
    )
    
    fetch_parser.add_argument(
    "-C", "--no-cache",
    action="store_true",
    help="Disable caching. If not specified, cache will be enabled with a filename derived from the base URL."
)


    analyze_parser = subparsers.add_parser("analyze", help="Analyze downloaded .mbx files.")
    analyze_parser.add_argument(
        "--input-dir",
        required=True,
        help="Directory containing the downloaded .mbx files to analyze.",
    )

    args = parser.parse_args()

    console = Console()

    if args.mode == "fetch-patches":
        if args.no_cache:
            cacheFileName = None
        else:
            sanitized_base_url = args.base_url.replace("https://", "").replace("/", "_").strip("_")
            cacheFileName = f"{sanitized_base_url}_cache.json"
        console.print("[bold blue]Fetching patches...[/bold blue]")
        fetch_and_parse_threads(args.base_url, args.end_date, args.start_date, args.output_dir, cacheFileName)

    elif args.mode == "analyze":
        console.print("[bold blue]Not implemented yet[/bold blue]")


    else:
        raise ValueError(f"Unknown mode specified: {args.mocd}")