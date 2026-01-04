def test_summarize(dispatch_all_sources):
    """Test the summarize function with all sources dispatched."""
    dispatcher = dispatch_all_sources
    dispatcher.save_results()
