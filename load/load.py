from source.data_manager import DataManager
from source.source_manager import Job
import logging
import xml.etree.ElementTree as ET
from xml.dom import minidom

logger = logging.getLogger(__name__)


class Load:

    def __init__(self, data_date: str | None = None):
        self.dm = DataManager(data_date)

    def process_jobs(self):
        """Process all jobs and generate gold layer files based on load configuration."""
        logger.info(f"Processing jobs for date: {self.dm.data_date}")

        if not self.dm.silver_dir.exists():
            logger.error(f"Silver directory does not exist: {self.dm.silver_dir}")
            return

        # Loop through each silver data file
        for source_name, silver_data in self.dm.iter_jsons(self.dm.silver_dir):
            # Load corresponding job
            job = self.dm.load_pickle(source_name, self.dm.jobs_dir)
            if job is None:
                logger.warning(f"No job found for {source_name}")
                continue

            # Check if this job has a load configuration
            if not hasattr(job, 'load') or not job.load:
                logger.info(f"No load configuration for {source_name}, skipping")
                continue

            # Process this job with its silver data
            logger.info(f"Loading {source_name}...")
            self.load(silver_data, job)

    def load(self, silver_data: dict, job: Job):
        """Generate gold layer files from silver data based on job load configuration.

        Args:
            silver_data: Silver data dictionary (structure: {source: str, result: {url: {fields}}})
            job: Job object with load configuration
        """
        if not job.load:
            logger.warning(f"No load configuration for job {job.name}")
            return

        source_name = silver_data.get("source", job.name)

        # Process XML output if configured
        if "xml" in job.load:
            self._generate_xml(silver_data, job.load["xml"], source_name)

        # Process JSON output if configured
        if "json" in job.load:
            self._generate_json(silver_data, job.load["json"], source_name)

    def _generate_xml(self, silver_data: dict, xml_config: dict, source_name: str):
        """Generate XML file from silver data.

        Args:
            silver_data: Silver data dictionary with result data
            xml_config: XML configuration with fields mapping
            source_name: Name of the source (used for filename)
        """
        logger.info(f"Generating XML file for {source_name}")

        fields_config = xml_config.get("fields", [])
        result_data = silver_data.get("result", {})
        extraction_date = silver_data.get("extraction_date")

        # Create root element with extraction_date
        root = ET.Element("feed")
        root.set("extraction_date", extraction_date)

        # Create an item for each URL in the result
        for url, data in result_data.items():
            item = ET.SubElement(root, "item")

            # Add each configured field
            for field_config in fields_config:
                field_name = field_config.get("field")
                xml_name = field_config.get("name", field_name)

                if field_name in data:
                    element = ET.SubElement(item, xml_name)
                    element.text = str(data[field_name])
                else:
                    logger.warning(f"Field '{field_name}' not found in data for {url}")

        # Pretty print XML
        xml_str = minidom.parseString(ET.tostring(root)).toprettyxml(indent="  ")

        # Save using DataManager
        self.dm.save_xml(xml_str, source_name, layer="gold")

    def _generate_json(self, silver_data: dict, json_config: dict, source_name: str):
        """Generate JSON file from silver data.

        Args:
            silver_data: Silver data dictionary with result data
            json_config: JSON configuration with fields mapping
            source_name: Name of the source (used for filename)
        """
        logger.info(f"Generating JSON file for {source_name}")

        fields_config = json_config.get("fields", [])
        result_data = silver_data.get("result", {})
        extraction_date = silver_data.get("extraction_date")
        source = silver_data.get("source", source_name)

        # Create result dict with filtered/mapped fields
        filtered_result = {}

        # Create an object for each URL in the result
        for url, data in result_data.items():
            output_data = {}

            # Add each configured field
            for field_config in fields_config:
                field_name = field_config.get("field")
                json_name = field_config.get("name", field_name)

                if field_name in data:
                    output_data[json_name] = data[field_name]
                else:
                    logger.warning(f"Field '{field_name}' not found in data for {url}")

            filtered_result[url] = output_data

        # Create output structure matching silver layer format
        output = {
            "source": source,
            "extraction_date": extraction_date,
            "result": filtered_result,
        }

        # Save using DataManager
        self.dm.save_json(output, source_name, layer="gold")
