from source.source_manager import Source, Nav, Job
from source.data_manager import DataManager
from extract.http import visit_html
from extract.rss import visit_rss
from lxml import html as lxml_html
from urllib.parse import urljoin, quote
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
from io import BytesIO
import pypdfium2 as pdfium
import sqlite3
import logging

logger = logging.getLogger(__name__)


class RunTracker:
    """Tracks fetched URLs in SQLite database to prevent duplicate fetches."""

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
        """Create the fetched_urls table if it doesn't exist."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS fetched_urls (
                    url TEXT PRIMARY KEY,
                    source_name TEXT NOT NULL,
                    fetch_datetime TEXT NOT NULL
                )
                """
            )
            # Create index for faster source lookups
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_source_name
                ON fetched_urls(source_name)
                """
            )
            conn.execute(
                """
                CREATE INDEX IF NOT EXISTS idx_fetch_datetime
                ON fetched_urls(fetch_datetime)
                """
            )
            conn.commit()

    def add_url(
        self, url: str, source_name: str, fetch_datetime: datetime = None
    ) -> None:
        """Add a fetched URL record to the database.

        Args:
            url: The URL that was fetched
            source_name: Name of the source
            fetch_datetime: DateTime of the fetch (defaults to now)
        """
        if fetch_datetime is None:
            fetch_datetime = datetime.now()

        datetime_str = fetch_datetime.isoformat()

        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                "INSERT OR IGNORE INTO fetched_urls (url, source_name, fetch_datetime) VALUES (?, ?, ?)",
                (url, source_name, datetime_str),
            )
            conn.commit()

    def has_been_fetched(self, url: str, source_name: str = None) -> bool:
        """Check if a URL has been fetched for a specific source.

        Args:
            url: The URL to check
            source_name: Optional source name. If provided, checks if this specific
                        source has fetched the URL. If None, checks globally.

        Returns:
            True if the URL has been fetched before, False otherwise
        """
        with sqlite3.connect(self.db_path) as conn:
            if source_name:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM fetched_urls WHERE url = ? AND source_name = ?",
                    (url, source_name),
                )
            else:
                cursor = conn.execute(
                    "SELECT COUNT(*) FROM fetched_urls WHERE url = ?",
                    (url,),
                )
            count = cursor.fetchone()[0]
            return count > 0

    def filter_unfetched_urls(self, urls: list[str], source_name: str) -> list[str]:
        """Filter out URLs that have already been fetched by this specific source.

        Args:
            urls: List of URLs to filter
            source_name: Name of the source doing the fetching

        Returns:
            List of URLs that have not been fetched by this source yet
        """
        if not urls:
            return []

        placeholders = ",".join("?" * len(urls))
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                f"SELECT url FROM fetched_urls WHERE url IN ({placeholders}) AND source_name = ?",
                (*urls, source_name),
            )
            fetched_urls = {row[0] for row in cursor.fetchall()}

        return [url for url in urls if url not in fetched_urls]

    def delete_by_source(self, source_name: str) -> int:
        """Delete all fetched URLs for a specific source.

        Args:
            source_name: Name of the source to delete

        Returns:
            Number of rows deleted
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                "DELETE FROM fetched_urls WHERE source_name = ?", (source_name,)
            )
            conn.commit()
            return cursor.rowcount

    def delete_by_url(self, url: str) -> int:
        """Delete a specific URL from the database.

        Args:
            url: The URL to delete

        Returns:
            Number of rows deleted
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM fetched_urls WHERE url = ?", (url,))
            conn.commit()
            return cursor.rowcount

    def delete_all(self) -> int:
        """Delete all fetched URLs from the database.

        Returns:
            Number of rows deleted
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute("DELETE FROM fetched_urls")
            conn.commit()
            return cursor.rowcount

    def reset_tracking_by_date(self, date_str: str = None) -> int:
        """Reset (delete) all fetched URLs for a specific date.

        Args:
            date_str: Date string in YYYY-MM-DD format. If None, uses today's date.

        Returns:
            Number of rows deleted
        """
        if date_str is None:
            target_date = datetime.now().strftime("%Y-%m-%d")
        else:
            target_date = date_str

        # Delete all records where the fetch_datetime date matches the target date
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                DELETE FROM fetched_urls
                WHERE date(fetch_datetime) = ?
                """,
                (target_date,),
            )
            conn.commit()
            return cursor.rowcount

    def get_latest_fetches(self, limit: int = 100) -> list[tuple[str, str, str]]:
        """Get the latest fetched URLs from the database.

        Args:
            limit: Maximum number of URLs to return (default: 100)

        Returns:
            List of tuples (url, source_name, fetch_datetime) ordered by most recent first
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.execute(
                """
                SELECT url, source_name, fetch_datetime
                FROM fetched_urls
                ORDER BY fetch_datetime DESC
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
        dm = DataManager()  # Uses today's date
        logger.info(f"Saving raw data for {self.source_name}")
        logger.info(f"Number of page results: {len(self.results)}")
        file_path = dm.save_json(self.to_json(), self.source_name, layer="raw")
        logger.info(f"Successfully saved {self.source_name} to {file_path}")


class Navigate:

    def __init__(self, path: str, source_name: str | None = None):
        self.jobs: list[Job] = Source(path, source_name=source_name).gen_jobs()

    def start(self):
        for job in self.jobs:
            if not job.nav:
                job.urls = [job.start]
                continue

            current_navs = [job.nav[0]]

            for step_index in range(len(job.nav)):
                is_final = step_index == len(job.nav) - 1
                current_navs = self.process_navigation_step(
                    job, current_navs, step_index, is_final
                )

    def process_navigation_step(
        self, job: Job, current_navs: list[Nav], step_index: int, is_final: bool
    ) -> list[Nav]:

        all_urls = self.navigate_all(current_navs)
        if not all_urls:
            logger.warning(f"No URLs found during navigation step {step_index + 1} for job {job.name}: {job.nav[step_index]}")
            return []

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
            if not doc:
                return []
            relative_urls = self.select_rss(doc, nav.selector)
        elif nav.ftype == "html":
            doc = visit_html(url=nav.url)
            if not doc:
                return []
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
        self.run_tracker = RunTracker()

    def execute_jobs(self):
        for job in self.navigate.jobs:
            logger.info(f"Processing job: {job.name}")

            if not job.urls:
                logger.warning(f"No URLs found for job: {job.name}")
                continue

            # Filter out URLs that have already been fetched by this source
            unfetched_urls = self.run_tracker.filter_unfetched_urls(job.urls, job.name)

            # Skip if all URLs have already been fetched by this source
            if not unfetched_urls:
                logger.info(f"All URLs already fetched for job: {job.name}")
                continue

            logger.info(f"Fetching {len(unfetched_urls)} URLs for {job.name}")
            page_results = []
            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for url in unfetched_urls:
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
                        # Mark this URL as fetched
                        self.run_tracker.add_url(result.url, job.name)

            logger.info(f"Collected {len(page_results)} page results for {job.name}")
            self.results.append(
                SourceResult(source_name=job.name, results=page_results)
            )

    def rss_extract(self, job: Job, url: str) -> PageResult:
        rss = visit_rss(url=url)
        if not rss:
            return None

        extractions = []
        for entry in rss.entries:
            for field in job.extract:
                data = entry.get(field.selector)
                if data:
                    extractions.append(Extraction(name=field.name, data=data.strip()))

        return PageResult(url=url, fields=extractions)

    def html_extract(self, job: Job, url: str) -> PageResult:
        html = visit_html(url=url)
        if not html:
            return None

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
        if not doc:
            return None

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
