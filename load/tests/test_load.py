import pytest
from load.load import Load
from source.data_manager import DataManager
from datetime import datetime
import xml.etree.ElementTree as ET


def test_load(dispatch_transform_all_sources):
    """Test that load creates gold layer files with correct structure."""
    # dispatch_transform_all_sources runs the full pipeline (dispatch + transform)
    # Now we run load to create gold layer files
    data_date = datetime.now().strftime("%Y-%m-%d")
    dm = DataManager(data_date)
    load = Load(data_date)

    # Process all jobs to create gold layer files
    load.process_jobs()

    # Test XML output for test_rss_html
    xml_data = dm.load_xml("test_rss_html", layer="gold")
    assert xml_data is not None, "XML file should be created for test_rss_html"

    # Parse and validate XML structure
    root = ET.fromstring(xml_data)
    assert root.tag == "feed", "Root element should be 'feed'"

    items = root.findall("item")
    assert len(items) > 0, "Should have at least one item"

    # Validate XML fields from config
    first_item = items[0]
    assert first_item.find("description") is not None, "Should have 'description' field"
    assert first_item.find("title") is not None, "Should have 'title' field"

    # Test JSON output for test_rss_html
    json_data = dm.load_json("test_rss_html", layer="gold")
    assert json_data is not None, "JSON file should be created for test_rss_html"
    assert isinstance(json_data, list), "JSON data should be a list of items"
    assert len(json_data) > 0, "Should have at least one item"

    # Validate JSON fields from config
    first_json_item = json_data[0]
    assert "title" in first_json_item, "Should have 'title' field"

    # Test JSON output for test_only_rss
    json_data_rss = dm.load_json("test_only_rss", layer="gold")
    assert json_data_rss is not None, "JSON file should be created for test_only_rss"
    assert isinstance(json_data_rss, list), "JSON data should be a list"
    assert len(json_data_rss) > 0, "Should have at least one item"

    # Validate fields mapping
    first_rss_item = json_data_rss[0]
    assert "title" in first_rss_item, "Should have 'title' field"
    assert "description" in first_rss_item, "Should have 'description' field"
