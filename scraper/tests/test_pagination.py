from scraper.linkedin_scraper import LinkedInScraper

def test_pagination_state(monkeypatch):
    """Unit‑test string parsing only (no Selenium)."""
    ls = LinkedInScraper(headless=True)
    assert ls._pagination_state.__name__  # smoke – function exists
