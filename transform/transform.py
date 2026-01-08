from source.source_manager import Job
from source.data_manager import DataManager
import logging
import os
from openai import OpenAI

logger = logging.getLogger(__name__)


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

    def transform(self, raw: dict, job: Job):
        """Transform a single job with its raw data using LLM steps.

        Args:
            raw: Raw data dictionary from JSON file (structure: {source: str, result: {url: {fields}}})
            job: Job object with configuration including transform.LLM steps
        """
        if not job.transform or "LLM" not in job.transform:
            logger.warning(f"No LLM steps defined for job {job.name}")
            return

        # Initialize OpenAI client
        api_key = os.getenv("OPENAI_API_KEY")
        if not api_key:
            logger.error("OPENAI_API_KEY environment variable not set")
            return

        client = OpenAI(api_key=api_key)
        llm_steps = job.transform["LLM"]

        # Process each URL's data
        result_data = raw.get("result", {})
        processed_results = {}

        for url, data in result_data.items():
            logger.info(f"Processing URL: {url}")
            processed_data = data.copy()

            # Apply each LLM step sequentially
            for llm_step in llm_steps:
                processed_data = self._process_llm_step(
                    processed_data, llm_step, client
                )

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
