
from rich.console import Console
from rich.table import Table
from rich import print
from datetime import timedelta
import pandas as pd
from collections import Counter
import datetime
from data_store import (
    thread_initiators, 
    thread_responders, 
    thread_response_counts, 
    thread_times, 
    patches_df,
    name_to_emails,     
    email_to_name,
    get_patches  
)

class ReportGenerator:
    def __init__(self, top_count=10):
        """
        Initialize the ReportGenerator.

        Args:
            top_count (int): Number of top entries to include in reports.
        """

        self.console = Console()
        self.top_count = top_count

    def print_date_range(self):
        """Print the date range of emails in the dataset."""
        patches_df = get_patches()
        try:
            oldest_date = pd.to_datetime(patches_df["Date"].min())
            newest_date = pd.to_datetime(patches_df["Date"].max())
            self.console.print(f"[bold cyan]Date Range:[/bold cyan] {oldest_date.date()} to {newest_date.date()}")
        except Exception as e:
            self.console.print(f"[bold red]Error calculating date range:[/bold red] {e}")

    def print_thread_initiators(self):
        """Print the top thread initiators."""
        self.console.print("\n[bold magenta]Top Thread Initiators:[/bold magenta]")
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Author", style="dim", width=40)
        table.add_column("Threads Initiated", justify="right")

        sorted_initiators = sorted(thread_initiators.items(), key=lambda x: x[1], reverse=True)
        for author, count in sorted_initiators[:self.top_count]:
            table.add_row(author, str(count))
        self.console.print(table)

    def print_thread_responders(self):
        """Print the top thread responders."""
        self.console.print("\n[bold magenta]Top Thread Responders:[/bold magenta]")
        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Author", style="dim", width=40)
        table.add_column("Threads Responded To", justify="right")

        sorted_responders = sorted(thread_responders.items(), key=lambda x: len(x[1]), reverse=True)
        for author, threads in sorted_responders[:self.top_count]:
            table.add_row(author, str(len(threads)))
        self.console.print(table)

    def print_avg_responses(self):
        """Print the average number of responses per thread."""
        total_responses = sum(thread_response_counts.values())
        avg_responses = total_responses / len(thread_response_counts) if thread_response_counts else 0
        self.console.print(f"\n[bold yellow]Average responses per thread:[/bold yellow] {avg_responses:.2f}")

    def print_avg_thread_duration(self):
        """Print the average thread duration in days and hours."""
        total_duration_seconds = 0
        valid_thread_count = 0
        
        for thread_id, times in thread_times.items():
            start_time, end_time = times
            
            # Ensure both start_time and end_time are valid integers
            if isinstance(start_time, int) and isinstance(end_time, int):
                duration = end_time - start_time
                
                # Only consider positive durations
                if duration >= 0:
                    total_duration_seconds += duration
                    valid_thread_count += 1
                else:
                    self.console.print(f"[bold red]Warning:[/bold red] Thread '{thread_id}' has negative duration. Skipping.")
            else:
                self.console.print(f"[bold red]Warning:[/bold red] Thread '{thread_id}' has invalid start or end time. Skipping.")
        
        if valid_thread_count > 0:
            avg_duration_seconds = total_duration_seconds / valid_thread_count
            
            # Convert seconds to days and hours
            avg_duration = timedelta(seconds=avg_duration_seconds)
            avg_days = avg_duration.days
            avg_hours = avg_duration.seconds // 3600
            self.console.print(f"[bold yellow]Average thread duration:[/bold yellow] {avg_days} days, {avg_hours} hours")
        else:
            self.console.print("[bold red]No valid thread durations found to calculate average.[/bold red]")


    def print_top_author_domains(self, top_n=10):
        """Print the top email domains among authors."""
        self.console.print("\n[bold green]Top Email Domains for Authors:[/bold green]")
        domain_counter = Counter()

        for author in thread_initiators:
            emails = name_to_emails.get(author, [])
            for email in emails:
                domain = self.extract_domain(email)
                if domain:
                    domain_counter[domain] += 1

        top_domains = domain_counter.most_common(top_n)

        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Domain", style="dim", width=40)
        table.add_column("Count", justify="right")

        for domain, count in top_domains:
            table.add_row(domain, str(count))

        self.console.print(table)

    def print_top_responder_domains(self, top_n=10):
        """Print the top email domains among responders."""
        self.console.print("\n[bold green]Top Email Domains for Responders:[/bold green]")
        domain_counter = Counter()

        for responder in thread_responders:
            emails = name_to_emails.get(responder, [])
            for email in emails:
                domain = self.extract_domain(email)
                if domain:
                    domain_counter[domain] += 1

        top_domains = domain_counter.most_common(top_n)

        table = Table(show_header=True, header_style="bold blue")
        table.add_column("Domain", style="dim", width=40)
        table.add_column("Count", justify="right")

        for domain, count in top_domains:
            table.add_row(domain, str(count))

        self.console.print(table)

    @staticmethod
    def extract_domain(email):
        """
        Extract the domain from an email address.

        Args:
            email (str): The email address.

        Returns:
            str: The domain part of the email, or None if invalid.
        """
        try:
            return email.split('@')[1].lower()
        except (IndexError, AttributeError):
            return None

    def generate_all_reports(self):
        """Generate all reports."""
        self.print_date_range()
        self.print_thread_initiators()
        self.print_thread_responders()
        self.print_avg_responses()
        self.print_avg_thread_duration()
        self.print_top_author_domains(top_n=self.top_count)
        self.print_top_responder_domains(top_n=self.top_count)
