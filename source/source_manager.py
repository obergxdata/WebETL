from __future__ import annotations
from pathlib import Path
from dataclasses import dataclass
import yaml
import pickle
from datetime import datetime


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
    analyze: list[dict] | None = None


@dataclass
class Nav:
    selector: str
    ftype: str
    url: str | None = None
    must_contain: list[str] | None = None


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
                        )
                    else:
                        nav = Nav(
                            url=None,
                            selector=navigate["selector"],
                            ftype=navigate["ftype"],
                            must_contain=navigate.get("must_contain"),
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
                    analyze=source_conf.get("analyze", []),
                )
            )

        # Automatically save jobs after generating them
        self.save_jobs()

        return self.jobs

    def save_jobs(self):
        """Save jobs as pickle files using absolute path: root/data/jobs/YYYY-MM-DD/name.pkl"""
        # Get the root directory (parent of source directory)
        source_dir = Path(__file__).parent
        root_dir = source_dir.parent
        today = datetime.now().strftime("%Y-%m-%d")

        # Create the date-based directory
        save_dir = root_dir / "data" / "jobs" / today
        save_dir.mkdir(parents=True, exist_ok=True)

        # Save each job as a pickle file
        for job in self.jobs:
            file_path = save_dir / f"{job.name}.pkl"
            with open(file_path, "wb") as f:
                pickle.dump(job, f)

        return save_dir

    def __getitem__(self, index):
        return self.jobs[index]
