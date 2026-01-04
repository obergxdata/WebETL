from job.gen_jobs import Source, Nav, Job
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
