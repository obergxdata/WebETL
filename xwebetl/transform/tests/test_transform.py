from datetime import datetime
from xwebetl.transform.transform import Transform
from xwebetl.extract.dispatch import Dispatcher
from pathlib import Path
import json

# get current date as string YYYY-MM-DD
DATA_DATE = datetime.now().strftime("%Y-%m-%d")


def test_transform(test_sources_yml):
    """Test basic LLM transformation."""
    dispatcher = Dispatcher(path=test_sources_yml, source_name="test_rss_html")
    dispatcher.execute_jobs()
    dispatcher.save_results()

    transformer = Transform(path=test_sources_yml, source_name="test_rss_html")
    transformer.process_jobs()

    with open(Path.cwd() / "data" / "silver" / DATA_DATE / "test_rss_html.json") as f:
        data = json.load(f)

    first_entry = list(data["result"].values())[0][0]
    assert "article_summary" in first_entry
    assert "title_sentiment" in first_entry


def test_skip_if_condition(test_sources_yml):
    """Test that skip_if conditions stop subsequent steps."""
    dispatcher = Dispatcher(path=test_sources_yml, source_name="test_skip_if_condition")
    dispatcher.execute_jobs()
    dispatcher.save_results()

    transformer = Transform(path=test_sources_yml, source_name="test_skip_if_condition")
    transformer.process_jobs()

    with open(Path.cwd() / "data" / "silver" / DATA_DATE / "test_skip_if_condition.json") as f:
        data = json.load(f)

    entries = list(data["result"].values())[0]
    for entry in entries[:2]:
        assert entry["relevance"] != "YES"
        assert "description_sentiment" not in entry
