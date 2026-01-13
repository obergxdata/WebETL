from pathlib import Path
from datetime import datetime
import json
import logging

logger = logging.getLogger(__name__)


class DataManager:
    """Centralized data loading and saving for the RSSIFY pipeline."""

    def __init__(self, data_date: str | None = None):
        """Initialize DataManager with a specific date or today's date.

        Args:
            data_date: Date string in YYYY-MM-DD format. If None, uses today's date.
        """
        self.data_date = data_date or datetime.now().strftime("%Y-%m-%d")
        self.root_dir = self._get_root_dir()
        self._setup_directories()

    def _get_root_dir(self) -> Path:
        """Get the project root directory.

        Uses the current working directory so that data is created in the user's
        project directory, not in the installed package location.
        """
        return Path.cwd()

    def _setup_directories(self):
        """Setup all data directory paths."""
        self.raw_dir = self.root_dir / "data" / "raw" / self.data_date
        self.silver_dir = self.root_dir / "data" / "silver" / self.data_date
        self.gold_dir = self.root_dir / "data" / "gold" / self.data_date

    def ensure_dir(self, directory: Path) -> Path:
        """Ensure a directory exists, creating it if necessary."""
        directory.mkdir(parents=True, exist_ok=True)
        return directory

    # JSON operations
    def save_json(
        self,
        data: dict | list,
        filename: str,
        layer: str = "raw",
        indent: int = 2,
    ) -> Path:
        """Save data as a JSON file.

        Args:
            data: Data to save (dict or list)
            filename: Name of the file (with or without .json extension)
            layer: Data layer - "raw", "silver", or "gold"
            indent: JSON indentation level

        Returns:
            Path to the saved file
        """
        directory = getattr(self, f"{layer}_dir")
        self.ensure_dir(directory)

        if not filename.endswith(".json"):
            filename = f"{filename}.json"

        file_path = directory / filename
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)

        logger.info(f"Saved JSON to {file_path}")
        return file_path

    def load_json(self, filename: str, layer: str = "raw") -> dict | list | None:
        """Load data from a JSON file.

        Args:
            filename: Name of the file (with or without .json extension)
            layer: Data layer - "raw", "silver", or "gold"

        Returns:
            Loaded data or None if file doesn't exist
        """
        directory = getattr(self, f"{layer}_dir")

        if not filename.endswith(".json"):
            filename = f"{filename}.json"

        file_path = directory / filename
        if not file_path.exists():
            logger.warning(f"JSON file not found: {file_path}")
            return None

        with open(file_path, "r", encoding="utf-8") as f:
            return json.load(f)

    def iter_jsons(self, directory: Path | None = None):
        """Iterate over all JSON files in a directory.

        Args:
            directory: Directory to iterate (defaults to raw_dir)

        Yields:
            Tuples of (filename_stem, loaded_data)
        """
        if directory is None:
            directory = self.raw_dir

        if not directory.exists():
            logger.warning(f"Directory does not exist: {directory}")
            return

        for json_file in directory.glob("*.json"):
            # Determine layer from directory
            if directory == self.raw_dir:
                layer = "raw"
            elif directory == self.silver_dir:
                layer = "silver"
            elif directory == self.gold_dir:
                layer = "gold"
            else:
                layer = "raw"

            data = self.load_json(json_file.name, layer)
            if data is not None:
                yield json_file.stem, data

    # XML operations
    def save_xml(
        self,
        xml_string: str,
        filename: str,
        layer: str = "gold",
    ) -> Path:
        """Save XML data to a file.

        Args:
            xml_string: XML content as string
            filename: Name of the file (with or without .xml extension)
            layer: Data layer - typically "gold"

        Returns:
            Path to the saved file
        """
        directory = getattr(self, f"{layer}_dir")
        self.ensure_dir(directory)

        if not filename.endswith(".xml"):
            filename = f"{filename}.xml"

        file_path = directory / filename
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(xml_string)

        logger.info(f"Saved XML to {file_path}")
        return file_path

    def load_xml(self, filename: str, layer: str = "gold") -> str | None:
        """Load XML data from a file.

        Args:
            filename: Name of the file (with or without .xml extension)
            layer: Data layer - typically "gold"

        Returns:
            XML content as string or None if file doesn't exist
        """
        directory = getattr(self, f"{layer}_dir")

        if not filename.endswith(".xml"):
            filename = f"{filename}.xml"

        file_path = directory / filename
        if not file_path.exists():
            logger.warning(f"XML file not found: {file_path}")
            return None

        with open(file_path, "r", encoding="utf-8") as f:
            return f.read()
