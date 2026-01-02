from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass
import yaml


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
    nav: list[Nav]
    urls: list[str] | None = None


@dataclass
class Nav:
    selector: str
    ftype: str
    url: str | None = None


class Source:

    def __init__(self, path: str):
        self.path = path
        self.sources = self.load_yml()
        self.jobs: list[Job] = []

    def load_yml(self):
        path = Path(self.path)
        with open(path, "r") as f:
            return yaml.safe_load(f)

    def gen_jobs(self):
        for source_conf in self.sources["source"]:

            fields = []
            for field in source_conf["extract"]["fields"]:
                fields.append(Field(name=field["name"], selector=field["selector"]))

            navs = []

            if "navigate" in source_conf:
                for i, navigate in enumerate(source_conf["navigate"]):
                    if i == 0:
                        nav = Nav(
                            url=source_conf["start"],
                            selector=navigate["selector"],
                            ftype=source_conf["ftype"],
                        )
                    else:
                        nav = Nav(
                            url=None,
                            selector=navigate["selector"],
                            ftype=navigate["ftype"],
                        )

                    navs.append(nav)

            self.jobs.append(
                Job(
                    name=source_conf["name"],
                    ftype=source_conf["ftype"],
                    extract=fields,
                    nav=navs,
                    start=source_conf["start"],
                )
            )

        return self.jobs

    def __getitem__(self, index):
        return self.jobs[index]
