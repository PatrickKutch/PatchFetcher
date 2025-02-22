from collections import defaultdict
#import pandas as pd

# Global data structures
thread_initiators = defaultdict(int)  # Author -> Number of threads initiated
thread_responders = defaultdict(set)  # Author -> Set of threads they responded to
thread_response_counts = defaultdict(int)  # ThreadID -> Number of responses
thread_times = {}  # ThreadID -> [start_time, last_response_time]
patches_df = None #pd.DataFrame() #= pd.DataFrame(columns=["PatchID", "Author", "ReviewedBy", "SignedOffBy","Date"])
# New data structures to map names to their email addresses
name_to_emails = defaultdict(set)  # Author Name -> Set of email addresses
email_to_name = {}  # Email Address -> Author Name


# For some reason, I CANNOT use patches_df as a global variable without the following 2 functions
def get_patches():
    return patches_df

def set_patches(df):
    global patches_df
    patches_df = df