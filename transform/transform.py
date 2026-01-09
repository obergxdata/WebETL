from source.source_manager import Job
from source.data_manager import DataManager
import logging
import os
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


class Transform:

    def __init__(self, data_date: str | None = None):
        self.dm = DataManager(data_date)

    def process_jobs(self):
        logger.info(f"Processing jobs for date: {self.dm.data_date}")
        if not self.dm.jobs_dir.exists():
            logger.error(f"Jobs directory does not exist: {self.dm.jobs_dir}")
            return

        # Loop through each job file
        for job_name, job in self.dm.iter_pickles(self.dm.jobs_dir):
            # Load corresponding raw data
            raw_data = self.dm.load_json(job_name, layer="raw")
            if raw_data is None:
                logger.warning(f"No raw data found for {job_name}")
                continue

            # If transform is False, save directly to silver (preserving extraction_date)
            if not job.transform:
                self.dm.save_json(raw_data, job_name, layer="silver")
                logger.info(f"Saved {job_name} to silver (no transform needed)")
                continue

            # Process this job with its raw data
            logger.info(f"Processing {job_name}...")
            self.transform(raw_data, job)

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
            logger.info(
                f"Calling OpenAI API for step '{llm_step['name']}' with model {llm_step['model']}"
            )
            response = client.chat.completions.create(
                model=llm_step["model"],
                messages=[
                    {"role": "system", "content": llm_step["prompt"]},
                    {"role": "user", "content": doc},
                ],
            )
            result = response.choices[0].message.content
            logger.info(f"Received response for step '{llm_step['name']}'")

            # Add the result to data with the output key
            data[llm_step["output"]] = result

        except Exception as e:
            logger.error(f"Error calling OpenAI API for step '{llm_step['name']}': {e}")

        return data

    def _process_url(
        self, url: str, data: dict, llm_steps: list[dict], client: OpenAI
    ) -> tuple[str, dict]:
        """Process a single URL through all LLM steps.

        Args:
            url: The URL being processed
            data: Dictionary with initial data fields for this URL
            llm_steps: List of LLM step configurations
            client: OpenAI client instance

        Returns:
            tuple: (url, processed_data) - The URL and its processed data
        """
        logger.info(f"Processing URL: {url}")
        processed_data = data.copy()

        # Apply each LLM step sequentially for this URL
        for llm_step in llm_steps:
            processed_data = self._process_llm_step(processed_data, llm_step, client)

        return url, processed_data

    def transform(self, raw: dict, job: Job):
        """Transform a single job with its raw data using LLM steps.

        Args:
            raw: Raw data dictionary from JSON file (structure: {source: str, result: {url: {fields}}})
            job: Job object with configuration including transform.LLM steps
        """

        if not job.transform:
            logger.warning(f"No transform steps defined for job {job.name}")
            return

        if "LLM" in job.transform:

            if not OPENAI_API_KEY:
                raise Exception("OPENAI_API_KEY environment variable not set")

            client = OpenAI(api_key=OPENAI_API_KEY)
            llm_steps = job.transform["LLM"]
        else:
            return

        # Process each URL's data using multithreading
        result_data = raw.get("result", {})
        processed_results = {}

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            for url, data in result_data.items():
                futures.append(
                    executor.submit(self._process_url, url, data, llm_steps, client)
                )

            for future in as_completed(futures):
                url, processed_data = future.result()
                processed_results[url] = processed_data

        # Create final output structure (preserve extraction_date from raw data)
        output = {
            "source": raw["source"],
            "extraction_date": raw["extraction_date"],
            "result": processed_results,
        }

        # Save to silver
        self.dm.save_json(output, job.name, layer="silver")
        logger.info(f"Saved transformed data for {job.name} to silver")
