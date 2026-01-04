from source.source_manager import Source, Nav, Job
from fetch.http import visit_html
from fetch.rss import visit_rss
from lxml import html as lxml_html
from urllib.parse import urljoin, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import json
from io import BytesIO
import pypdfium2 as pdfium
import sqlite3


class RunTracker:
    """Tracks source runs in SQLite database."""

    def __init__(self, db_path: str | Path = None):
        if db_path is None:
            project_root = Path(__file__).parent.parent
            db_path = project_root / "data" / "runs.db"
        else:
            db_path = Path(db_path)

        # Ensure data directory exists
        db_path.parent.mkdir(parents=True, exist_ok=True)

        self.db_path = db_path
        self._create_table()

    def _create_table(self) -> None:
        """Create the runs table if it doesn't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source_name TEXT NOT NULL,
                    run_datetime TEXT NOT NULL
                )
                """
            )
            # Create index for faster lookups
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_source_name
                ON runs(source_name)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_run_datetime
                ON runs(run_datetime)
                """
            )
            conn.commit()

    def add_run(self, source_name: str, run_datetime: datetime = None) -> None:
        """Add a run record to the database.

        Args:
            source_name: Name of the source
            run_datetime: DateTime of the run (defaults to now)
        """
        if run_datetime is None:
            run_datetime = datetime.now()

        datetime_str = run_datetime.isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT INTO runs (source_name, run_datetime) VALUES (?, ?)",
                (source_name, datetime_str),
            )
            conn.commit()

    def delete_by_source(self, source_name: str) -> int:
        """Delete all runs for a specific source.

        Args:
            source_name: Name of the source to delete

        Returns:
            Number of rows deleted
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM runs WHERE source_name = ?", (source_name,)
            )
            conn.commit()
            return cursor.rowcount

    def delete_by_date(self, date: datetime | str = None) -> int:
        """Delete all runs for a specific date.

        Args:
            date: Date to delete (datetime object, date string "YYYY-MM-DD", or None for today)

        Returns:
            Number of rows deleted
        """
        if date is None:
            date = datetime.now()

        # Handle string input (e.g., "2026-01-04")
        if isinstance(date, str):
            date_str = date
        else:
            # Handle both datetime and date objects
            date_str = date.strftime("%Y-%m-%d")

        start_datetime = f"{date_str}T00:00:00"
        end_datetime = f"{date_str}T23:59:59"

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM runs WHERE run_datetime >= ? AND run_datetime <= ?",
                (start_datetime, end_datetime),
            )
            conn.commit()
            return cursor.rowcount

    def delete_all(self) -> int:
        """Delete all runs from the database.

        Returns:
            Number of rows deleted
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM runs")
            conn.commit()
            return cursor.rowcount

    def has_run_today(self, source_name: str) -> bool:
        """Check if a source has run today.

        Args:
            source_name: Name of the source to check

        Returns:
            True if the source has run today, False otherwise
        """
        today = datetime.now().strftime("%Y-%m-%d")
        start_datetime = f"{today}T00:00:00"
        end_datetime = f"{today}T23:59:59"

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT COUNT(*) FROM runs
                WHERE source_name = ?
                AND run_datetime >= ?
                AND run_datetime <= ?
                """,
                (source_name, start_datetime, end_datetime),
            )
            count = cursor.fetchone()[0]
            return count > 0

    def get_latest_runs(self, limit: int = 100) -> list[tuple[str, str]]:
        """Get the latest runs from the database.

        Args:
            limit: Maximum number of runs to return (default: 100)

        Returns:
            List of tuples (source_name, run_datetime) ordered by most recent first
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT source_name, run_datetime
                FROM runs
                ORDER BY run_datetime DESC
                LIMIT ?
                """,
                (limit,),
            )
            return cursor.fetchall()


@dataclass
class Extraction:
    name: str
    data: str


@dataclass
class PageResult:
    url: str
    fields: list[Extraction]


@dataclass
class SourceResult:
    source_name: str
    results: list[PageResult]

    def to_json(self) -> dict:
        result_dict = {}
        for page_result in self.results:
            fields_dict = {}
            for extraction in page_result.fields:
                fields_dict[extraction.name] = extraction.data
            result_dict[page_result.url] = fields_dict

        return {"source": self.source_name, "result": result_dict}

    def save(self) -> None:
        today = datetime.now().strftime("%Y-%m-%d")
        # Use absolute path from the project root (where conftest.py is located)
        project_root = Path(__file__).parent.parent
        output_dir = project_root / "data" / "raw" / today
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / f"{self.source_name}.json"
        with open(file_path, "w") as f:
            json.dump(self.to_json(), f, indent=2)


class Navigate:

    def __init__(self, path: str, source_name: str | None = None):
        self.jobs: list[Job] = Source(path, source_name=source_name).gen_jobs()
        self.run_tracker = RunTracker()

    def start(self):
        for job in self.jobs:
            # Skip if this source has already run today
            if self.run_tracker.has_run_today(job.name):
                continue

            if not job.nav:
                job.urls = [job.start]
                continue

            current_navs = [job.nav[0]]

            for step_index in range(len(job.nav)):
                is_final = step_index == len(job.nav) - 1
                current_navs = self.process_navigation_step(
                    job, current_navs, step_index, is_final
                )

            # Mark this source as run after successful navigation
            self.run_tracker.add_run(job.name)

    def process_navigation_step(
        self, job: Job, current_navs: list[Nav], step_index: int, is_final: bool
    ) -> list[Nav]:

        all_urls = self.navigate_all(current_navs)
        if not all_urls:
            raise Exception(f"Failed to navigate: {job.nav[step_index]}")

        if is_final:
            if job.urls is None:
                job.urls = []
            job.urls.extend(all_urls)
            return []
        else:
            return self.build_next_navs(all_urls, job.nav[step_index + 1])

    def navigate_all(self, navs: list[Nav]) -> list[str]:
        all_urls = []

        with ThreadPoolExecutor(max_workers=10) as executor:
            future_to_nav = {executor.submit(self.navigate, nav): nav for nav in navs}

            for future in as_completed(future_to_nav):
                urls = future.result()
                all_urls.extend(urls)

        return all_urls

    def build_next_navs(self, urls: list[str], template: Nav) -> list[Nav]:
        return [
            Nav(
                url=url,
                selector=template.selector,
                ftype=template.ftype,
                must_contain=template.must_contain,
            )
            for url in urls
        ]

    def filter_urls(self, urls: list[str], nav: Nav) -> list[str]:
        if not nav.must_contain:
            return urls
        return [
            url for url in urls if all(pattern in url for pattern in nav.must_contain)
        ]

    def navigate(self, nav: Nav) -> list[str]:

        if nav.ftype == "rss":
            doc = visit_rss(url=nav.url)
            relative_urls = self.select_rss(doc, nav.selector)
        elif nav.ftype == "html":
            doc = visit_html(url=nav.url)
            relative_urls = self.select_html(doc, nav.selector)
        else:
            raise Exception(f"Unsupported navigation ftype: {nav.ftype}")

        relative_urls = self.filter_urls(relative_urls, nav)

        return [
            urljoin(nav.url, quote(url, safe="/:?#[]@!$&'()*+,;="))
            for url in relative_urls
        ]

    def select_html(self, doc: str, selector: str) -> list:
        tree = lxml_html.fromstring(doc)
        return tree.xpath(selector)

    def select_rss(self, doc: str, selector: str) -> list:
        return [entry.get(selector) for entry in doc.entries]


class Dispatcher:

    def __init__(self, path: str, source_name: str | None = None):
        self.navigate = Navigate(path, source_name=source_name)
        self.navigate.start()
        self.results: list[SourceResult] = []

    def execute_jobs(self):
        for job in self.navigate.jobs:

            if not job.urls:
                continue
            page_results = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for url in job.urls:
                    if job.extract_ftype == "html":
                        extractor = self.html_extract
                    elif job.extract_ftype == "rss":
                        extractor = self.rss_extract
                    elif job.extract_ftype == "pdf":
                        extractor = self.pdf_extract
                    else:
                        raise Exception(f"Unsupported job ftype: {job.ftype}")

                    futures.append(executor.submit(extractor, job, url))

                for future in as_completed(futures):
                    result = future.result()
                    if result:
                        page_results.append(result)

            self.results.append(
                SourceResult(source_name=job.name, results=page_results)
            )

    def rss_extract(self, job: Job, url: str) -> PageResult:
        rss = visit_rss(url=url)

        extractions = []
        for entry in rss.entries:
            for field in job.extract:
                data = entry.get(field.selector)
                if data:
                    extractions.append(Extraction(name=field.name, data=data.strip()))

        return PageResult(url=url, fields=extractions)

    def html_extract(self, job: Job, url: str) -> PageResult:
        html = visit_html(url=url)
        tree = lxml_html.fromstring(html)

        extractions = []
        for field in job.extract:
            data = tree.xpath(field.selector)
            if data:
                value = data[0] if isinstance(data[0], str) else data[0].text_content()
                extractions.append(Extraction(name=field.name, data=value.strip()))

        return PageResult(url=url, fields=extractions)

    def pdf_extract(self, job: Job, url: str) -> list:
        # Load PDF from bytes
        doc = visit_html(url=url, text=False)
        pdf = pdfium.PdfDocument(BytesIO(doc))

        extractions = []
        for page_num in range(len(pdf)):
            page = pdf[page_num]
            textpage = page.get_textpage()
            text = textpage.get_text_range()
            extractions.append(text)
            textpage.close()
            page.close()

        pdf.close()

        return PageResult(
            url=url, fields=[Extraction(name="content", data="".join(extractions))]
        )

    def save_results(self) -> None:
        for source_result in self.results:
            source_result.save()
