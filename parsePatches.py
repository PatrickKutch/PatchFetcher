import os
import re
import time
import argparse
import pandas as pd
from rich import print
from rich.progress import Progress, BarColumn, TimeRemainingColumn
from rich.table import Table
from rich.console import Console
from collections import defaultdict
from datetime import datetime
from dateutil import parser
from datetime import timedelta, timezone

from data_store import thread_initiators, thread_responders, thread_response_counts, thread_times, patches_df
from data_store import name_to_emails, email_to_name, set_patches
from generateReports import ReportGenerator

patches_data = []
patches_data_buffer = []

def print_metrics(top_count=10):
    """Generate and print reports."""
    report_gen = ReportGenerator(top_count=top_count)
    report_gen.generate_all_reports()



def extract_field_nuke(line, field_name):
    """
    Extracts a specific field from a line in the .mbx file.

    Args:
        line: A single line from the .mbx file.
        field_name: The name of the field to extract.

    Returns:
        The extracted field value, or an empty string if not found.
    """
    prefix = f"{field_name}:"
    if line.startswith(prefix):
        return line[len(prefix):].strip()
    return ""

def generate_reviewer_author_table(top_n=10):
    """
    Generate and print a table showing the top reviewers for the specified number of top authors,
    with a separator line after each author.

    Args:
        top_n (int): Number of top authors to display (default: 10).
    """
    console = Console()

    # Sort authors by the number of threads they initiated
    top_authors = sorted(thread_initiators.items(), key=lambda x: x[1], reverse=True)[:top_n]

    if not top_authors:
        console.print("[bold red]No authors found in thread_initiators![/bold red]")
        return

    # Table for displaying heatmap data
    table = Table(show_header=True, header_style="bold blue")
    table.add_column("Author", style="dim", width=40)
    table.add_column("Top Reviewers (with count)", justify="left", width=70)

    for author, _ in top_authors:
        # Collect reviewers for the author's threads
        reviewers = []
        threads = thread_responders.get(author, set())
        for thread_id in threads:
            for responder, responded_threads in thread_responders.items():
                if thread_id in responded_threads and responder != author:
                    reviewers.append(responder)

        # Count and sort reviewers by frequency
        reviewer_counts = {r: reviewers.count(r) for r in set(reviewers)}
        sorted_reviewers = sorted(reviewer_counts.items(), key=lambda x: x[1], reverse=True)

        # Format top reviewers for the table
        top_reviewers = ", ".join([f"{reviewer} ({count})" for reviewer, count in sorted_reviewers[:5]])

        # Add row to the table
        table.add_row(author, top_reviewers or "No reviewers")

        # Add a blank row as a separator
        table.add_row("", "")

    console.print(table)

    
def parse_emails_from_mbx(file_path):
    """
    Parses emails from a .mbx file, extracting key metadata fields and 'Reviewed-by' lines.

    Args:
        file_path (str): Path to the .mbx file.

    Returns:
        List[Dict[str, Any]]: A list of dictionaries, each containing email metadata.
    """
    emails = []
    current_email = None
    skipList = ["syzbot", "patchwork-bot"]

    try:
        with open(file_path, "r", encoding="utf-8", errors="replace") as file:
            for line in file:
                if line.startswith("From mboxrd@z"):
                    # Start a new email if a "From:" line is encountered
                    if current_email:
                        if not any(skip_item in current_email["From"][0] for skip_item in skipList):
                            emails.append(current_email)

                    current_email = {
                        "From": (None, None),
                        "To": "",
                        "Date": "",
                        "Subject": "",
                        "ReviewedBy": [],
                        # "Body": "",
                    }

                elif line.startswith("Subject:") and current_email and not current_email["Subject"]:
                    subject_lines = [extract_field(line, "Subject")[0]]
                    while True:
                        next_line = next(file).strip()
                        if not next_line :
                            break
                        if next_line.startswith(("From:", "To:", "Date:", "Reviewed-by:")):                        
                            break
                        subject_lines.append(next_line)
                    current_email["Subject"] = " ".join(subject_lines)
                    if next_line:
                        line = next_line
                    

                if line.startswith("From:") and current_email and not current_email["From"][0]:
                    name, email = extract_field(line, "From")
                    current_email["From"] = (name, email)
                    if name:
                        if email:
                            name_to_emails[name].add(email)
                            email_to_name[email] = name
                        else:
                            # Handle cases where email might be missing
                            name_to_emails[name].add("unknown@example.com")  # Placeholder or handle appropriately

                elif line.startswith("To:") and current_email and not current_email["To"]:
                    name, email = extract_field(line, "To")
                    if name:
                        if email:
                            name_to_emails[name].add(email)
                            email_to_name[email] = name
                        current_email["To"] = name  # Store only the name for analysis

                if line.startswith("Date:") and current_email and not current_email["Date"]:
                    current_email["Date"] = extract_field(line, "Date")[0]  # Date remains as string
                    
                elif line.startswith("Reviewed-by:") and current_email:
                    name, email = extract_field(line, "Reviewed-by")
                    if name:
                        if email:
                            name_to_emails[name].add(email)
                            email_to_name[email] = name
                        current_email["ReviewedBy"].append(name)  # Store only the name for analysis

                # Handle other fields if necessary

            # Append the last email if it exists
            if current_email:
                if not any(skip_item in current_email["From"][0] for skip_item in skipList):
                    emails.append(current_email)


    except Exception as e:
        print(f"Error reading file {file_path}: {e}")

    return emails



def extract_field(line, field_name):
    """
    Extracts the name and email from a line in the .mbx file.

    Args:
        line (str): A single line from the .mbx file.
        field_name (str): The name of the field to extract.

    Returns:
        tuple: (name, email) if found, else (None, None).
    """
    line = line.strip()
    pattern = fr"{field_name}:\s*(.*)"
    match = re.match(pattern, line)
    if match:
        content = match.group(1).strip()
        # Regex to extract name and email
        name_email_match = re.match(r'(.*?)\s*<(.+?)>', content)
        if name_email_match:
            name = name_email_match.group(1).strip()
            email = name_email_match.group(2).strip().lower()
            return name, email
        else:
            # If no email is present, return the whole content as name
            return content, None
    return None, None


def parse_date(date_str, thread_id):
    """
    Parses a date string into a timezone-aware datetime object.

    Args:
        date_str (str): The date string to parse.

    Returns:
        datetime: A timezone-aware datetime object, or None if parsing fails.
    """
    # Define common timezone mappings
    orig_str = date_str
    tzinfos = {
        "CEST": timezone(timedelta(hours=2)),  # Central European Summer Time
        "CET": timezone(timedelta(hours=1)),   # Central European Time
        "PST": timezone(timedelta(hours=-8)),  # Pacific Standard Time
        "PDT": timezone(timedelta(hours=-7)),  # Pacific Daylight Time
        "GMT": timezone(timedelta(hours=0)),   # Greenwich Mean Time
    }

    try:
        # Preprocess the date string
        if '\t' in date_str:
            # Split the string at the tab and keep the part before it
            date_str =  date_str.split('\t')[0]
            
        date_str = date_str.strip()
        
        date_str = re.sub(r'\s+', ' ', date_str)  # Collapse multiple spaces
        date_str = re.sub(r'=\S+', '', date_str)  # Remove encoded characters
        date_str = re.sub(r'\(.*?\)', '', date_str)  # Remove content in parentheses
#        date_str = date_str.replace("at ", "").replace("/", "-")

        # Remove invalid leading/trailing characters
        date_str = re.sub(r'^[^\w]+', '', date_str)
        date_str = re.sub(r'[^\w]+$', '', date_str)

        # Attempt to parse the date string
        parsed_date = parser.parse(date_str, tzinfos=tzinfos)

        # Force the result to be timezone-aware
        if parsed_date.tzinfo is None:
            parsed_date = parsed_date.replace(tzinfo=timezone.utc)

        return int(parsed_date.timestamp())

    except Exception as e:
        print(f"Error parsing date for {thread_id}: {date_str} [{orig_str}]. Exception: {e}")
        return None



def update_patches_data(emails, file_path):
    """
    Updates thread data and metrics based on email data.

    Args:
        emails: List of email data dictionaries, where the first email is the thread initiator.
    """
    global patches_df, thread_initiators, thread_responders, thread_response_counts, thread_times

    if not emails:
        return

    # Extract the thread initiator
    initiator_email = emails[0]
    thread_author_name = initiator_email.get("From", (None, None))[0]
    thread_author_email = initiator_email.get("From", (None, None))[1]

    if not thread_author_name:
        thread_author_name = "Unknown Author"

    thread_id = initiator_email.get("Subject", "").strip()  # Use the subject of the first email as a thread ID placeholder

    # Validate thread_id
    if not thread_id:
        print(f"Warning: Missing thread ID for file {file_path}")
        return

    thread_start_date = parse_date(initiator_email.get("Date", ""), file_path)
    # Track the thread initiator by name
    thread_initiators[thread_author_name] += 1

    # Initialize thread timing
    thread_times.setdefault(thread_id, [thread_start_date, thread_start_date])  # [start_time, last_response_time]

    # Update thread timing with a valid start date
    if thread_start_date:
        thread_times[thread_id][0] = thread_start_date

    # Process all emails in the thread
    for idx, email in enumerate(emails):
        author_name = email.get("From", (None, None))[0]
        author_email = email.get("From", (None, None))[1]
        date = parse_date(email.get("Date", ""), file_path)
        if not date:
            date = thread_start_date
        reviewers = email.get("ReviewedBy", [])

        if not author_name:
            author_name = "Unknown Author"

        # Update thread timing with the most recent response
        if date:
            try:
                thread_times[thread_id][1] = max(thread_times[thread_id][1], date)
            except TypeError:
                print(f"Invalid date comparison in thread '{thread_id}': {thread_times[thread_id][1]} vs {date}")
                continue

        # Increment response count for the thread (excluding the initiator responding to their own thread)
        if idx > 0 and author_name != thread_author_name:
            thread_response_counts[thread_id] += 1
            thread_responders[author_name].add(thread_id)

        # Update the global DataFrame for reporting
        row = {
            "From": author_name,  # Use name instead of email
            "To": email.get("To", ""),  # Assuming 'To' is now a name
            "Date": email.get("Date", ""),
            "Subject": thread_id,  # Use thread ID for consistency
            "ReviewedBy": ", ".join(reviewers),  # Join list of reviewers as a single string
            # "Body": email.get("Body", "").strip(),  # Strip trailing newlines or spaces
        }

        patches_data_buffer.append(row)
    # patches_df = pd.concat([patches_df, pd.DataFrame([row])], ignore_index=True)


def finalize_patches_data():
    global patches_df, patches_data_buffer
    
    patches_df = pd.DataFrame(patches_data_buffer)
    set_patches(patches_df)
    patches_data_buffer = []  # Clear the buffer

def process_file(file_path):
    """
    Processes a single .mbx file and returns parsed patch data.
    
    Args:
        file_path: Path to the .mbx file.

    Returns:
        List of patch dictionaries from the file.
    """
    # if '_PATCH_net_v1__net__stmmac__TSO__Fix_unbalanced_DMA_map_unmap_for_non-paged_SKB_data' not in file_path:
    #     return []

    return parse_emails_from_mbx(file_path)

def walk_and_process(input_dir, file_limit=None):
    """
    Walks through the input directory to find .mbx files
    and processes each one sequentially with progress tracking.

    Args:
        input_dir: The base directory to search.
        file_limit: Number of files to process. If None or negative, process all files.
    """
    print(f"Scanning input directory: {input_dir}")
    if not os.path.isdir(input_dir):
        print(f"Error: {input_dir} is not a valid directory.")
        return

    # Gather all .mbx file paths
    mbx_files = []
    for root, dirs, files in os.walk(input_dir):
        for file in files:
            if file.endswith(".mbx"):
                file_path = os.path.join(root, file)
                mbx_files.append(file_path)

    # If file_limit is specified and positive, slice the list
    if file_limit and file_limit > 0:
        mbx_files = mbx_files[:file_limit]

    print(f"Found {len(mbx_files)} .mbx files to process.")

    # Add progress bar
    with Progress(
        "[progress.description]{task.description}",
        BarColumn(),
        "[progress.percentage]{task.percentage:>3.0f}%",
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task("[cyan]Processing .mbx files...", total=len(mbx_files))

        # Process files sequentially and update the global DataFrame
        for file_path in mbx_files:
            emails = process_file(file_path)
            update_patches_data(emails, file_path)
            progress.update(task, advance=1)

    finalize_patches_data()
    print_metrics(10)

  

def main():
    """
    Main function to handle command-line arguments and start the process.
    """
    parser = argparse.ArgumentParser(description="Process .mbx files in a directory.")
    parser.add_argument(
        "--input-dir",
        type=str,
        default="b4_threads",
        help="Base directory to scan for .mbx files (default: b4_threads)."
    )
    args = parser.parse_args()

    # Start the timer
    start_time = time.time()

    # Process the input directory
    walk_and_process(args.input_dir)

    # Stop the timer and calculate the elapsed time
    elapsed_time = time.time() - start_time

    # Print the total runtime
    print(f"Program completed in {elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    main()
