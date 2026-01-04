from datetime import datetime
from analyze.analyze import Analyze

# get current date as string YYYY-MM-DD
DATA_DATE = datetime.now().strftime("%Y-%m-%d")


def test_summarize(dispatch_all_sources):
    """Test the summarize function with all sources dispatched."""
    dispatcher = dispatch_all_sources
    dispatcher.save_results()
    analyzer = Analyze(data_date=DATA_DATE)

    # Verify only jobs with analyze=True were loaded (only test_rss_html has it)
    assert len(analyzer.jobs) == 1
    assert "test_rss_html" in analyzer.jobs
    assert len(analyzer.raw_data) == 1
    assert "test_rss_html" in analyzer.raw_data

    # Verify the job has analyze field
    assert analyzer.jobs["test_rss_html"].analyze is not None
    assert len(analyzer.jobs["test_rss_html"].analyze) > 0

    # Call summarize
    analyzer.run()
