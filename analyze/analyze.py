from source.source_manager import Job
from pathlib import Path
import pickle
import json
import logging
import os
from openai import OpenAI

logger = logging.getLogger(__name__)


class Analyze:

    def __init__(self, data_date: str):
        self.data_date = data_date
        # Get absolute paths to data directories
        analyze_dir = Path(__file__).parent
        root_dir = analyze_dir.parent
        self.jobs_dir = root_dir / "data" / "jobs" / self.data_date
        self.raw_data_dir = root_dir / "data" / "raw" / self.data_date
        self.silver_data_dir = root_dir / "data" / "silver" / self.data_date
        # Create silver directory if it doesn't exist
        self.silver_data_dir.mkdir(parents=True, exist_ok=True)

    def _load_job(self, pkl_file: Path) -> Job | None:
        """Load a single job from a pickle file.

        Args:
            pkl_file: Path to the pickle file

        Returns:
            Job object if analyze is truthy, None otherwise
        """
        with open(pkl_file, "rb") as f:
            job = pickle.load(f)
            return job
        return None

    def _load_raw_data(self, source_name: str) -> dict | None:
        """Load raw data JSON file for a single source.

        Args:
            source_name: Name of the source

        Returns:
            dict: Raw data dictionary, or None if file doesn't exist
        """
        json_file = self.raw_data_dir / f"{source_name}.json"
        if json_file.exists():
            with open(json_file, "r") as f:
                return json.load(f)
        return None

    def _save_to_silver(self, job_name: str, raw_data: dict):
        """Save raw data directly to silver directory without analysis.

        Args:
            job_name: Name of the job/source
            raw_data: Raw data to save
        """
        silver_file = self.silver_data_dir / f"{job_name}.json"
        with open(silver_file, "w") as f:
            json.dump(raw_data, f, indent=2)
        logger.info(f"Saved {job_name} to silver (no analysis needed)")

    def process_jobs(self):
        logger.info(f"Processing jobs for date: {self.data_date}")
        if not self.jobs_dir.exists():
            logger.error(f"Jobs directory does not exist: {self.jobs_dir}")
            return

        # Loop through each job file
        for pkl_file in self.jobs_dir.glob("*.pkl"):
            job_name = pkl_file.stem  # filename without extension

            # Load the job
            job = self._load_job(pkl_file)
            if job is None:
                continue

            # Load corresponding raw data
            raw_data = self._load_raw_data(job_name)
            if raw_data is None:
                logger.warning(
                    f"No raw data found for {job_name} at {self.raw_data_dir / f'{job_name}.json'}"
                )
                continue

            # If analyze is False, save directly to silver
            if not job.analyze:
                self._save_to_silver(job_name, raw_data)
                continue

            # Process this job with its raw data
            logger.info(f"Processing {job_name}...")
            self.analyze(raw_data, job)

    def _process_llm_step(self, data: dict, llm_step: dict, client: OpenAI) -> dict:
        """Process a single LLM step on the data.

        Args:
            data: Dictionary with current data fields
            llm_step: LLM step configuration with input, output, model, and prompt
            client: OpenAI client instance

        Returns:
            dict: Updated data with new output field added
        """
        # Build document string from input fields
        doc_parts = []
        for field in llm_step["input"]:
            if field in data:
                doc_parts.append(f"{field}: {data[field]}")
            else:
                logger.warning(f"Field '{field}' not found in data, skipping")

        if not doc_parts:
            logger.warning(f"No input fields found for LLM step '{llm_step['name']}'")
            return data

        doc = "\n".join(doc_parts)

        # Call OpenAI API
        try:
            logger.info(f"Calling OpenAI API for step '{llm_step['name']}' with model {llm_step['model']}")
            response = client.chat.completions.create(
                model=llm_step["model"],
                messages=[
                    {"role": "system", "content": llm_step["prompt"]},
                    {"role": "user", "content": doc}
                ]
            )
            result = response.choices[0].message.content
            logger.info(f"Received response for step '{llm_step['name']}'")

            # Add the result to data with the output key
            data[llm_step["output"]] = result

        except Exception as e:
            logger.error(f"Error calling OpenAI API for step '{llm_step['name']}': {e}")

        return data

    def analyze(self, raw: dict, job: Job):
        """Analyze a single job with its raw data using LLM steps.

        Args:
            raw: Raw data dictionary from JSON file (structure: {source: str, result: {url: {fields}}})
            job: Job object with configuration including analyze.LLM steps
        """
        if not job.analyze or "LLM" not in job.analyze:
            logger.warning(f"No LLM steps defined for job {job.name}")
            return

        # Initialize OpenAI client
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("OPENAI_API_KEY environment variable not set")
            return

        client = OpenAI(api_key=api_key)
        llm_steps = job.analyze["LLM"]

        # Process each URL's data
        result_data = raw.get("result", {})
        processed_results = {}

        for url, data in result_data.items():
            logger.info(f"Processing URL: {url}")
            processed_data = data.copy()

            # Apply each LLM step sequentially
            for llm_step in llm_steps:
                processed_data = self._process_llm_step(processed_data, llm_step, client)

            processed_results[url] = processed_data

        # Create final output structure
        output = {
            "source": raw["source"],
            "result": processed_results
        }

        # Save to silver
        silver_file = self.silver_data_dir / f"{job.name}.json"
        with open(silver_file, "w") as f:
            json.dump(output, f, indent=2)
        logger.info(f"Saved analyzed data for {job.name} to silver")
