from datetime import datetime
from analyze.analyze import Analyze

# get current date as string YYYY-MM-DD
DATA_DATE = datetime.now().strftime("%Y-%m-%d")


def test_summarize(dispatch_all_sources):
    """Test the summarize function with all sources dispatched."""
    dispatcher = dispatch_all_sources
    dispatcher.save_results()
    analyzer = Analyze(data_date=DATA_DATE)

    # Verify jobs and raw data were loaded
    assert len(analyzer.jobs) == 4
    assert len(analyzer.raw_data) == 4

    # Call summarize
    analyzer.summarize()
