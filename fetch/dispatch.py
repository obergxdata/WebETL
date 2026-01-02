from job.gen_jobs import Source, Nav, Job
from fetch.http import visit_html
from fetch.rss import visit_rss
from lxml import html as lxml_html
from urllib.parse import urljoin
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime
import json


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
        output_dir = Path(f"data/raw/{today}")
        output_dir.mkdir(parents=True, exist_ok=True)

        file_path = output_dir / f"{self.source_name}.json"
        with open(file_path, "w") as f:
            json.dump(self.to_json(), f, indent=2)


class Navigate:

    def __init__(self, path: str):
        self.jobs: list[Job] = Source(path).gen_jobs()

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
            Nav(url=url, selector=template.selector, ftype=template.ftype)
            for url in urls
        ]

    def navigate(self, nav: Nav) -> list[str]:

        if nav.ftype == "rss":
            doc = visit_rss(url=nav.url)
            relative_urls = self.select_rss(doc, nav.selector)
        elif nav.ftype == "html":
            doc = visit_html(url=nav.url)
            relative_urls = self.select_html(doc, nav.selector)

        return [urljoin(nav.url, url) for url in relative_urls]

    def select_html(self, doc: str, selector: str) -> list:
        tree = lxml_html.fromstring(doc)
        return tree.xpath(selector)

    def select_rss(self, doc: str, selector: str) -> list:
        return [entry.get(selector) for entry in doc.entries]


class Dispatcher:

    def __init__(self, path: str):
        self.navigate = Navigate(path)
        self.navigate.start()
        self.results: list[SourceResult] = []

    def execute_jobs(self):
        for job in self.navigate.jobs:
            page_results = []

            with ThreadPoolExecutor(max_workers=10) as executor:
                futures = []
                for url in job.urls:
                    if job.ftype == "html":
                        extractor = self.html_extract
                    elif job.ftype == "rss":
                        extractor = self.rss_extract

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
