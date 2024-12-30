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


def fetch_thread_with_b4(thread_url, base_url, thread_title, output_dir="b4_threads",retry=b4_retry_count):
    """Fetch a thread using the b4 CLI tool."""
    full_url = f"{base_url}{thread_url.lstrip('/')}"
    thread_id = thread_url.rstrip('/').split("/")[-2]
    if len(thread_title) > 200:
        orig_title = thread_title
        thread_title = thread_title[:200]
        truncated = True
    else:
        truncated = False
        
    sanitized_title = ''.join(c if c.isalnum() or c in ('-', '_') else '_' for c in thread_title).replace(' ', '_').strip()

    thread_dir = os.path.join(output_dir, sanitized_title)

    if os.path.exists(thread_dir):
        #console.print(f"[yellow]Directory already exists with .mbx file, skipping b4 fetch: {thread_dir}[/yellow]")
        return thread_dir
  
    try:
        result = subprocess.run(
            ["b4", "am", full_url, "-C", "-o", thread_dir],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
         
        try:
            fp = open(thread_dir +"/b4_call","wt")
            fp.write(f"b4 am {full_url} -C -o {thread_dir}")
            fp.write("\n")
        
            if truncated:
                fp = open(thread_dir +"/b4_call","at")
                fp.write("Thread title was too long, so it was truncated\n")
                fp.write(orig_title)
                fp.write("\n")
            fp.close()
        except Exception:
            pass
        #console.print(f"[green]Fetched thread: {full_url}[/green]" )
        return thread_dir
    
    except subprocess.CalledProcessError as e:
        if not retry or 'That message-id is not known.' in e.stderr:
            console.print(f"[red]Error fetching thread: {full_url}[/red]  {thread_dir}")
            console.print(f"[red]{e.stderr}[/red]")
            if not os.path.exists(thread_dir):
                os.mkdir(thread_dir)
            fp = open(thread_dir +"/error.txt","wt")
            fp.write(e.stderr)
            fp.close()
            
            fp = open(thread_dir +"/b4_call","wt")
            fp.write(f"b4 am {full_url} -C -o {thread_dir}")
            fp.write("\n")
        
            if truncated:
                fp = open(thread_dir +"/b4_call","at")
                fp.write("Thread title was too long, so it was truncated\n")
                fp.write(orig_title)
                fp.write("\n")
            fp.close()

            return None
        else:
            if '503' in e.stderr:
                #console.print(f"[yellow] b4 timeout.  Sleeping[/yellow]")
                pass
            else:
                #console.print(f"[red] b4 error.  {e.stderr}[/red] -> {thread_dir}")
                pass
            
            time.sleep(b4_retry_interval)
            retry -=1
            return fetch_thread_with_b4(thread_url,base_url, thread_title, output_dir,retry)


def parse_mbox(thread_dir):
    """Parse the .mbx file in the thread directory for patches and discussions."""
    try:
        # Find the .mbx file in the directory
        mbox_files = [f for f in os.listdir(thread_dir) if f.endswith(".mbx")]
        if not mbox_files:
            console.print(f"[red]No .mbox file found in directory: {thread_dir}[/red]")
            return []

        mbox_file = os.path.join(thread_dir, mbox_files[0])  # Assume the first .mbox file
        with open(mbox_file, "r") as f:
            content = f.read()
            return [{"content": content}]  # Basic placeholder, extend as needed
    except Exception as e:
        console.print(f"[red]Error parsing mbox in directory: {thread_dir}[/red]")
        console.print(f"[red]{e}[/red]")
        return []

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
    from rich.progress import Progress

    with Progress() as progress:
        progress_task = progress.add_task("[cyan]Fetching threads...", total=100)

        while next_page:
            #console.print(f"[bold blue]Fetching page:[/bold blue] {next_page}")

            try:
                soup = get_page(next_page)
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 503:
                    #console.print(f"[yellow]503 Service Unavailable: Retrying after delay...[/yellow]")
                    time.sleep(read_http_sleep)  # Wait for nn seconds before retrying
                    continue
            except Exception as e:
                console.print(f"[red]Error fetching page: {next_page}[/red]")
                console.print(f"[red]{e}[/red]")
                break

            topic_threads = extract_topic_threads(soup)
            #console.print(f"[green]Found {len(topic_threads)} topic threads on page.[/green]")
            for thread_info in topic_threads:
                if thread_info[0] not in cachedTopics:
                    cachedTopics[thread_info[0]] = 1
                    thread_data.append(thread_info)
                else:
                    pass  # ignore duplicates - they will be older
            
            if cacheFileName and not next_page == base_url:
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
                            checkForCachedData = False
                            timestamp = str(oldestCachedPage)
                            page_date = datetime.strptime(timestamp, "%Y%m%d%H%M%S")
                            next_page = next_page.split("t=")[0] + "t=" + timestamp
                        
                        # Update progress
                        elapsed_time = (start_date - page_date).total_seconds()
                        progress_percentage = min(100, max(0, (elapsed_time / total_time_range) * 100))
                        progress.update(progress_task, completed=progress_percentage)

                        if page_date < cutoff_date:
                            console.print(f"[yellow]Reached cutoff date: {page_date}[/yellow]")
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
    if start_date_obj > oldest_date_obj:
        raise ValueError(f"start_date ({start_date}) must be newer than oldest_date ({oldest_date}).")
    
    
    os.makedirs(output_dir, exist_ok=True)
    thread_data = fetch_all_threads(base_url, start_date, oldest_date,cacheFileName)
    all_threads = []

    from concurrent.futures import ThreadPoolExecutor

    def process_thread(thread):
        thread_url, thread_title, base_url = thread
        return fetch_thread_with_b4(thread_url, base_url, thread_title, output_dir)
    
    total_threads = len(thread_data)
    processed_threads = 0
    with Progress() as progress:
        task = progress.add_task("[blue]Fetching and parsing threads...", total=total_threads)

        with ThreadPoolExecutor(max_workers=b4_thread_count) as executor:
            for thread_dir in executor.map(process_thread, ((thread[0], thread[1], base_url) for thread in thread_data)):
                processed_threads += 1
                progress.update(task, advance=1)  # Increment the progress bar


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
        "--oldest-date",
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
        fetch_and_parse_threads(args.base_url, args.start_date, args.oldest_date, args.output_dir, cacheFileName)

    elif args.mode == "analyze":
        console.print("[bold blue]Not implemented yet[/bold blue]")


    else:
        raise ValueError(f"Unknown mode specified: {args.mocd}")