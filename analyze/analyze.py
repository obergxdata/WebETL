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

        # Get absolute path to data/raw/YYYY-MM-DD directory
        analyze_dir = Path(__file__).parent
        root_dir = analyze_dir.parent
        raw_data_dir = root_dir / "data" / "raw" / self.data_date

        raw_data = {}

        # Load all JSON files in the directory
        if raw_data_dir.exists():
            for json_file in raw_data_dir.glob("*.json"):
                source_name = json_file.stem  # filename without extension
                with open(json_file, "r") as f:
                    raw_data[source_name] = json.load(f)

        return raw_data

    def _load_jobs(self) -> dict[str, Job]:
        """Load job pickles from data/jobs/YYYY-MM-DD/*.pkl files.

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
                    jobs[job_name] = pickle.load(f)

        return jobs

    def summarize(self):
        # Get all jobs where analyze contains a summarize type
        summarize_jobs = [
            job
            for job in self.jobs.values()
            if job.analyze and any(a.get("type") == "summarize" for a in job.analyze)
        ]


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
