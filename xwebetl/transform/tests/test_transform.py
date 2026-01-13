from datetime import datetime
from xwebetl.transform.transform import Transform
from pathlib import Path
import json

# get current date as string YYYY-MM-DD
DATA_DATE = datetime.now().strftime("%Y-%m-%d")


def test_transform(dispatch_all_sources, test_sources_yml):
    """Test the transform function with all sources dispatched."""
    dispatcher = dispatch_all_sources
    dispatcher.save_results()
    transformer = Transform(path=test_sources_yml)
    transformer.process_jobs()

    # Verify silver files exist and contain expected keys
    root_dir = Path(__file__).parent.parent.parent
    silver_dir = root_dir / "data" / "silver" / DATA_DATE

    # Test test_rss_html - should have LLM analysis with article_summary and title_sentiment
    test_rss_html_file = silver_dir / "test_rss_html.json"
    assert test_rss_html_file.exists(), "test_rss_html.json should exist in silver"

    with open(test_rss_html_file, "r") as f:
        test_rss_html_data = json.load(f)

    # Check that result exists and has URLs
    assert "result" in test_rss_html_data
    assert len(test_rss_html_data["result"]) > 0, "Should have at least one URL result"

    # Check each URL's data has the LLM-generated keys
    for url, entries in test_rss_html_data["result"].items():
        assert isinstance(entries, list), f"Data for {url} should be a list of entries"
        assert len(entries) > 0, f"Should have at least one entry for {url}"
        # Check the first entry has all the fields
        entry = entries[0]
        assert "article_summary" in entry, f"article_summary should exist for {url}"
        assert "title_sentiment" in entry, f"title_sentiment should exist for {url}"
        assert "title" in entry, f"Original title field should still exist for {url}"
        assert "body" in entry, f"Original body field should still exist for {url}"

    # Test test_only_rss - should have LLM analysis with title_sentiment
    test_only_rss_file = silver_dir / "test_only_rss.json"
    assert test_only_rss_file.exists(), "test_only_rss.json should exist in silver"

    with open(test_only_rss_file, "r") as f:
        test_only_rss_data = json.load(f)

    # Check that result exists and has URLs
    assert "result" in test_only_rss_data
    assert len(test_only_rss_data["result"]) > 0, "Should have at least one URL result"

    # Check each URL's data has the LLM-generated key
    for url, entries in test_only_rss_data["result"].items():
        assert isinstance(entries, list), f"Data for {url} should be a list of entries"
        assert len(entries) > 0, f"Should have at least one entry for {url}"
        # Check all entries have the required fields
        for i, entry in enumerate(entries):
            assert "title_sentiment" in entry, f"title_sentiment should exist for {url} entry {i}"
            assert "title" in entry, f"Original title field should still exist for {url} entry {i}"

    # Test sources without analyze should still be saved to silver
    test_file = silver_dir / "test.json"
    assert test_file.exists(), "test.json should exist in silver (no analysis needed)"

    test_rss_html_pdf_file = silver_dir / "test_rss_html_pdf.json"
    assert (
        test_rss_html_pdf_file.exists()
    ), "test_rss_html_pdf.json should exist in silver (no analysis needed)"
