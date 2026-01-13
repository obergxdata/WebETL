import pytest
from xwebetl.load.load import Load
from xwebetl.source.data_manager import DataManager
from datetime import datetime
import xml.etree.ElementTree as ET


def test_load(dispatch_transform_all_sources, test_sources_yml):
    """Test that load creates gold layer files with correct structure."""
    # dispatch_transform_all_sources runs the full pipeline (dispatch + transform)
    # Now we run load to create gold layer files
    data_date = datetime.now().strftime("%Y-%m-%d")
    dm = DataManager(data_date)
    load = Load(path=test_sources_yml)

    # Process all jobs to create gold layer files
    load.process_jobs()

    # Test XML output for test_rss_html
    xml_data = dm.load_xml("test_rss_html", layer="gold")
    assert xml_data is not None, "XML file should be created for test_rss_html"

    # Parse and validate XML structure
    root = ET.fromstring(xml_data)
    assert root.tag == "feed", "Root element should be 'feed'"
    assert root.get("extraction_date") is not None, "Feed should have extraction_date attribute"

    items = root.findall("item")
    assert len(items) > 0, "Should have at least one item"

    # Validate XML fields from config
    first_item = items[0]
    assert first_item.find("description") is not None, "Should have 'description' field"
    assert first_item.find("title") is not None, "Should have 'title' field"

    # Test JSON output for test_rss_html
    json_data = dm.load_json("test_rss_html", layer="gold")
    assert json_data is not None, "JSON file should be created for test_rss_html"
    assert isinstance(json_data, dict), "JSON data should be a dict with metadata"
    assert "source" in json_data, "JSON should have source"
    assert "extraction_date" in json_data, "JSON should have extraction_date"
    assert "result" in json_data, "JSON should have result"
    assert isinstance(json_data["result"], dict), "JSON result should be a dict"
    assert len(json_data["result"]) > 0, "Should have at least one result item"

    # Validate JSON fields from config (get first URL's data)
    first_url_entries = list(json_data["result"].values())[0]
    assert isinstance(first_url_entries, list), "URL data should be a list of entries"
    assert len(first_url_entries) > 0, "Should have at least one entry"
    first_entry = first_url_entries[0]
    assert "title" in first_entry, "Should have 'title' field"

    # Test JSON output for test_only_rss
    json_data_rss = dm.load_json("test_only_rss", layer="gold")
    assert json_data_rss is not None, "JSON file should be created for test_only_rss"
    assert isinstance(json_data_rss, dict), "JSON data should be a dict with metadata"
    assert "source" in json_data_rss, "JSON should have source"
    assert "extraction_date" in json_data_rss, "JSON should have extraction_date"
    assert "result" in json_data_rss, "JSON should have result"
    assert len(json_data_rss["result"]) > 0, "Should have at least one result item"

    # Validate fields mapping
    first_rss_entries = list(json_data_rss["result"].values())[0]
    assert isinstance(first_rss_entries, list), "URL data should be a list of entries"
    assert len(first_rss_entries) > 0, "Should have at least one entry"
    first_rss_entry = first_rss_entries[0]
    assert "title" in first_rss_entry, "Should have 'title' field"
    assert "description" in first_rss_entry, "Should have 'description' field"
