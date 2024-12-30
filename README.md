# Fetch Patches

Fetches patches from a public mailing list such as  https://lore.kernel.org/netdev/ using the B4 utility. Storing the information locally for processing later.

The utility will parse the html pages, keeping track of all patch links, and then it will use the B4 utility to go pull info for those patch links.  By default, it will create a cache file for those links, as it may take a long time to go fetch them all, if you specify a long period of time.

If the app crashes, is interrupted or connectivty is lost, when you re-run, using the cache file will save a lot of time. 

```
usage: fetchPatches.py fetch-patches [-h] --base-url BASE_URL [--start-date START_DATE] --oldest-date OLDEST_DATE [--output-dir OUTPUT_DIR] [-C]

optional arguments:
  -h, --help            show this help message and exit
  --base-url BASE_URL   Base URL to fetch threads from, e.g., https://lore.kernel.org/netdev/
  --start-date START_DATE
                        Start from a specific date. e.g., e.g., 2024-12-01. Default is to start from now
  --oldest-date OLDEST_DATE
                        Oldest date to fetch threads for, e.g., 2024-12-02.
  --output-dir OUTPUT_DIR
                        Directory to save fetched mbx files.
  -C, --no-cache        Disable caching. If not specified, cache will be enabled with a filename derived from the base URL.
```