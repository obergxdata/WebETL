from xwebetl.source.source_manager import Job
from xwebetl.source.base_processor import BaseProcessor
import logging
import os
from openai import OpenAI
from concurrent.futures import ThreadPoolExecutor, as_completed

logger = logging.getLogger(__name__)
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


class Transform(BaseProcessor):

    def _get_input_layer(self) -> str:
        """Get the data layer to read from."""
        return "raw"

    def _should_process(self, job: Job, job_name: str, data: dict) -> bool:
        """Check if this job should be transformed."""
        # If transform is False, save directly to silver (preserving extraction_date)
        if not job.transform:
            self.dm.save_json(data, job_name, layer="silver")
            logger.info(f"Saved {job_name} to silver (no transform needed)")
            return False
        return True

    def _process(self, job_name: str, data: dict, job: Job):
        """Transform the raw data to silver."""
        self.transform(data, job)

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

    def _process_entry(
        self,
        url: str,
        entry_index: int,
        entry: dict,
        llm_steps: list[dict],
        client: OpenAI,
    ) -> tuple[str, int, dict]:
        """Process a single entry through all LLM steps.

        Args:
            url: The source URL this entry came from
            entry_index: The index of this entry in the source
            entry: Dictionary with initial data fields for this entry
            llm_steps: List of LLM step configurations
            client: OpenAI client instance

        Returns:
            tuple: (url, entry_index, processed_entry)
        """
        logger.info(f"Processing entry {entry_index} from URL: {url}")
        processed_entry = entry.copy()

        # Apply each LLM step sequentially for this entry
        for llm_step in llm_steps:
            # Check skip_if condition before processing
            if "skip_if" in llm_step:
                skip_condition = llm_step["skip_if"]
                field = skip_condition.get("field")
                not_equals = skip_condition.get("not_equals")

                if field and not_equals:
                    field_value = processed_entry.get(field)
                    if field_value != not_equals:
                        logger.info(
                            f"Skipping step '{llm_step['name']}' and all subsequent steps: "
                            f"field '{field}' = '{field_value}' (expected '{not_equals}')"
                        )
                        # Stop processing all subsequent steps
                        break

            processed_entry = self._process_llm_step(processed_entry, llm_step, client)

        return url, entry_index, processed_entry

    def transform(self, raw: dict, job: Job):
        """Transform a single job with its raw data using LLM steps.

        Args:
            raw: Raw data dictionary from JSON file (structure: {source: str, result: {url: [{fields}, ...]}})
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

        # Process each entry using multithreading
        result_data = raw.get("result", {})
        processed_results = {}

        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = []
            # Submit a task for each entry across all URLs
            for url, entries in result_data.items():
                # Initialize the result structure for this URL
                processed_results[url] = [None] * len(entries)

                for entry_index, entry in enumerate(entries):
                    futures.append(
                        executor.submit(
                            self._process_entry,
                            url,
                            entry_index,
                            entry,
                            llm_steps,
                            client,
                        )
                    )

            # Collect results as they complete
            for future in as_completed(futures):
                url, entry_index, processed_entry = future.result()
                processed_results[url][entry_index] = processed_entry

        # Create final output structure (preserve extraction_date from raw data)
        output = {
            "source": raw["source"],
            "extraction_date": raw["extraction_date"],
            "result": processed_results,
        }

        # Save to silver
        self.dm.save_json(output, job.name, layer="silver")
        logger.info(f"Saved transformed data for {job.name} to silver")
