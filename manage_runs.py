#!/usr/bin/env python3
"""CLI tool for managing fetched URLs tracking."""

import sys
from fetch.dispatch import RunTracker


def main():
    tracker = RunTracker()

    if len(sys.argv) < 2:
        print("Usage: python manage_runs.py <command> [args]")
        print("\nCommands:")
        print("  last-fetches [limit]       - Show latest fetched URLs (default: 100)")
        print("  has-fetched <url>          - Check if URL has been fetched")
        print("  delete-source <source>     - Delete all fetched URLs for a source")
        print("  delete-url <url>           - Delete a specific URL")
        print("  delete-all                 - Delete all fetched URLs")
        sys.exit(1)

    command = sys.argv[1]

    if command == "last-fetches":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        fetches = tracker.get_latest_fetches(limit=limit)
        if not fetches:
            print("No fetched URLs found")
        else:
            for url, source_name, fetch_datetime in fetches:
                print(f"{fetch_datetime}\t{source_name}\t{url}")

    elif command == "has-fetched":
        if len(sys.argv) < 3:
            print("Usage: python manage_runs.py has-fetched <url>")
            sys.exit(1)
        url = sys.argv[2]
        result = tracker.has_been_fetched(url)
        print("yes" if result else "no")
        sys.exit(0 if result else 1)

    elif command == "delete-source":
        if len(sys.argv) < 3:
            print("Usage: python manage_runs.py delete-source <source_name>")
            sys.exit(1)
        source_name = sys.argv[2]
        count = tracker.delete_by_source(source_name)
        print(f"Deleted {count} URLs for {source_name}")

    elif command == "delete-url":
        if len(sys.argv) < 3:
            print("Usage: python manage_runs.py delete-url <url>")
            sys.exit(1)
        url = sys.argv[2]
        count = tracker.delete_by_url(url)
        print(f"Deleted URL: {url}" if count > 0 else f"URL not found: {url}")

    elif command == "delete-all":
        count = tracker.delete_all()
        print(f"Deleted {count} URLs")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
