#!/usr/bin/env python3
"""CLI tool for managing source runs tracking."""

import sys
from fetch.dispatch import RunTracker


def main():
    tracker = RunTracker()

    if len(sys.argv) < 2:
        print("Usage: python manage_runs.py <command> [args]")
        print("\nCommands:")
        print("  last-runs [limit]          - Show latest runs (default: 100)")
        print("  has-run-today <source>     - Check if source has run today")
        print("  delete-source <source>     - Delete all runs for a source")
        print("  delete-date [YYYY-MM-DD]   - Delete runs for a date (default: today)")
        print("  delete-all                 - Delete all runs")
        sys.exit(1)

    command = sys.argv[1]

    if command == "last-runs":
        limit = int(sys.argv[2]) if len(sys.argv) > 2 else 100
        runs = tracker.get_latest_runs(limit=limit)
        if not runs:
            print("No runs found")
        else:
            for source_name, run_datetime in runs:
                print(f"{run_datetime}\t{source_name}")

    elif command == "has-run-today":
        if len(sys.argv) < 3:
            print("Usage: python manage_runs.py has-run-today <source_name>")
            sys.exit(1)
        source_name = sys.argv[2]
        result = tracker.has_run_today(source_name)
        print("yes" if result else "no")
        sys.exit(0 if result else 1)

    elif command == "delete-source":
        if len(sys.argv) < 3:
            print("Usage: python manage_runs.py delete-source <source_name>")
            sys.exit(1)
        source_name = sys.argv[2]
        count = tracker.delete_by_source(source_name)
        print(f"Deleted {count} runs for {source_name}")

    elif command == "delete-date":
        date = sys.argv[2] if len(sys.argv) > 2 else None
        count = tracker.delete_by_date(date)
        print(f"Deleted {count} runs")

    elif command == "delete-all":
        count = tracker.delete_all()
        print(f"Deleted {count} runs")

    else:
        print(f"Unknown command: {command}")
        sys.exit(1)


if __name__ == "__main__":
    main()
