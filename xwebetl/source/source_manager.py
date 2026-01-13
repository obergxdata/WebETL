from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass
import yaml
from xwebetl.source.data_manager import DataManager


@dataclass
class Field:
    name: str
    selector: str


@dataclass
class Job:
    name: str
    start: str
    ftype: str
    extract: list[Field]
    extract_ftype: str
    nav: list[Nav]
    urls: list[str] | None = None
    transform: list[dict] | None = None
    load: dict | None = None
    no_track: bool = False


@dataclass
class Nav:
    selector: str
    ftype: str
    url: str | None = None
    must_contain: list[str] | None = None
    must_contain_all: list[str] | None = None
    max_items: int | None = None


class Source:

    def __init__(self, path: str, source_name: str | None = None):
        self.path = path
        self.source_name = source_name
        self.sources = self.load_yml()
        self.jobs: list[Job] = []

    def load_yml(self):
        path = Path(self.path)
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def gen_jobs(self):
        sources = self.sources["source"]

        if self.source_name:
            sources = [s for s in sources if s["name"] == self.source_name]
            if not sources:
                available_sources = [s["name"] for s in self.sources["source"]]
                raise ValueError(
                    f"Source '{self.source_name}' not found in config. "
                    f"Available sources: {', '.join(available_sources)}"
                )

        for source_conf in sources:

            fields = []
            if "fields" in source_conf["extract"]:
                for field in source_conf["extract"]["fields"]:
                    fields.append(Field(name=field["name"], selector=field["selector"]))

            navs = []

            if "navigate" in source_conf:

                for i, navigate in enumerate(source_conf["navigate"]):
                    if i == 0:
                        job_ftype = navigate["ftype"]
                        nav = Nav(
                            url=source_conf["start"],
                            selector=navigate["selector"],
                            ftype=navigate["ftype"],
                            must_contain=navigate.get("must_contain"),
                            must_contain_all=navigate.get("must_contain_all"),
                            max_items=navigate.get("max_items"),
                        )
                    else:
                        nav = Nav(
                            url=None,
                            selector=navigate["selector"],
                            ftype=navigate["ftype"],
                            must_contain=navigate.get("must_contain"),
                            must_contain_all=navigate.get("must_contain_all"),
                            max_items=navigate.get("max_items"),
                        )

                    navs.append(nav)
            else:
                job_ftype = source_conf["extract"]["ftype"]

            self.jobs.append(
                Job(
                    name=source_conf["name"],
                    ftype=job_ftype,
                    extract_ftype=source_conf["extract"]["ftype"],
                    extract=fields,
                    nav=navs,
                    start=source_conf["start"],
                    transform=source_conf.get("transform", []),
                    load=source_conf.get("load", None),
                    no_track=source_conf.get("no_track", False),
                )
            )

        return self.jobs

    def __getitem__(self, index):
        return self.jobs[index]
