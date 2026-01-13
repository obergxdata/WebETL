from xwebetl.source.source_manager import Job, Source
from xwebetl.source.data_manager import DataManager
import logging

logger = logging.getLogger(__name__)


class BaseProcessor:
    """Base class for processing jobs in the ETL pipeline.

    Provides common functionality for iterating through jobs and processing them.
    Subclasses must implement _process_single_job to define specific processing logic.
    """

    def __init__(self, path: str, data_date: str | None = None, source_name: str | None = None):
        """Initialize the processor.

        Args:
            path: Path to the YAML configuration file
            data_date: Date string in YYYY-MM-DD format. If None, uses today's date.
            source_name: Optional specific source/job name to process. If None, processes all jobs.
        """
        self.dm = DataManager(data_date)
        self.source_name = source_name
        self.jobs: list[Job] = Source(path, source_name=source_name).gen_jobs()

    def process_jobs(self):
        """Process jobs based on configuration.

        If source_name is specified, processes only that job.
        Otherwise, processes all jobs loaded from the YAML config.
        """
        logger.info(f"Processing {len(self.jobs)} job(s) for date: {self.dm.data_date}")

        # Loop through each job
        for job in self.jobs:
            self._process_single_job(job.name, job)

    def _process_single_job(self, job_name: str, job: Job):
        """Process a single job by loading data and calling the processing method.

        Args:
            job_name: Name of the job/source
            job: Job object with configuration
        """
        # Load data from the appropriate layer
        data = self.dm.load_json(job_name, layer=self._get_input_layer())
        if data is None:
            logger.warning(f"No {self._get_input_layer()} data found for {job_name}")
            return

        # Check if this job should be processed
        if not self._should_process(job, job_name, data):
            return

        # Process the job with its data
        logger.info(f"Processing {job_name}...")
        self._process(job_name, data, job)

    def _get_input_layer(self) -> str:
        """Get the data layer to read from.

        Must be implemented by subclasses.

        Returns:
            Layer name (e.g., 'raw', 'silver')
        """
        raise NotImplementedError("Subclasses must implement _get_input_layer")

    def _should_process(self, job: Job, job_name: str, data: dict) -> bool:
        """Determine if this job should be processed.

        Can be overridden by subclasses to add custom logic.

        Args:
            job: Job object with configuration
            job_name: Name of the job/source
            data: Loaded data

        Returns:
            True if the job should be processed, False otherwise
        """
        return True

    def _process(self, job_name: str, data: dict, job: Job):
        """Process the job with its data.

        Must be implemented by subclasses to define specific processing logic.

        Args:
            job_name: Name of the job/source
            data: Loaded data dictionary
            job: Job object with configuration
        """
        raise NotImplementedError("Subclasses must implement _process")
