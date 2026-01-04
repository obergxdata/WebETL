from source.source_manager import Job
from pathlib import Path
import pickle
import json


class Analyze:

    def __init__(self, data_date: str):
        self.data_date = data_date
        self.jobs = self._load_jobs()
        self.raw_data = self._load_raw_data()

    def _load_raw_data(self) -> dict[str, dict]:
        """Load raw data JSON files for sources that have analyze enabled.
        Only loads data for source names that exist in self.jobs.

        Returns:
            dict[str, dict]: Dictionary mapping source name to raw data
        """
        # Get absolute path to data/raw/YYYY-MM-DD directory
        analyze_dir = Path(__file__).parent
        root_dir = analyze_dir.parent
        raw_data_dir = root_dir / "data" / "raw" / self.data_date

        raw_data = {}

        # Only load raw data for sources that have jobs with analyze=True
        if raw_data_dir.exists():
            for source_name in self.jobs.keys():
                json_file = raw_data_dir / f"{source_name}.json"
                if json_file.exists():
                    with open(json_file, "r") as f:
                        raw_data[source_name] = json.load(f)

        return raw_data

    def _load_jobs(self) -> dict[str, Job]:
        """Load job pickles from data/jobs/YYYY-MM-DD/*.pkl files.
        Only loads jobs where analyze field is truthy.

        Returns:
            dict[str, Job]: Dictionary mapping job name to Job object
        """
        # Get absolute path to data/jobs/YYYY-MM-DD directory
        analyze_dir = Path(__file__).parent
        root_dir = analyze_dir.parent
        jobs_dir = root_dir / "data" / "jobs" / self.data_date

        jobs = {}

        # Load all pickle files in the directory
        if jobs_dir.exists():
            for pkl_file in jobs_dir.glob("*.pkl"):
                job_name = pkl_file.stem  # filename without extension
                with open(pkl_file, "rb") as f:
                    job = pickle.load(f)
                    # Only include jobs where analyze is truthy
                    if job.analyze:
                        jobs[job_name] = job

        return jobs

    def run(self):
        for job_name, job in self.jobs.items():
            for analyze_job in job.analyze:
                if analyze_job["type"] == "summarize":
                    result = self.summarize(job=job, ajob=analyze_job)

    def summarize(self, job: Job, ajob: dict):
        data = ""
        for url, result in self.raw_data[job.name]["result"].items():
            for field, value in result.items():
                if field in ajob["fields"]:
                    data += f"{field}: {value}\n"

            # Add sum logic here brother!
            # Then save to silver
            raise Exception(data)


"""
from transformers import AutoTokenizer, AutoModelForSeq2SeqLM

# Load the FLAN-T5 large model and tokenizer
model_name = "google/flan-t5-large"
tokenizer = AutoTokenizer.from_pretrained(model_name)
model = AutoModelForSeq2SeqLM.from_pretrained(model_name)

# Your article text
article = lol

# Create the prompt
prompt = f"Summarize this article: {article}"

# Tokenize the input
inputs = tokenizer(prompt, return_tensors="pt", max_length=512, truncation=True)

# Generate the summary
outputs = model.generate(
    inputs.input_ids,
    max_length=150,  # Maximum length of the summary
    min_length=40,   # Minimum length of the summary
    length_penalty=2.0,
    num_beams=4,
    early_stopping=True
)

# Decode and print the summary
summary = tokenizer.decode(outputs[0], skip_special_tokens=True)
print("Summary:")
print(summary)"""
