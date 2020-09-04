import pytest


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    """Remove requests.sessions.Session.request for all tests.

    See (https://docs.pytest.org/en/stable/monkeypatch.html#global-patch-example-
       preventing-requests-from-remote-operations)
    """
    monkeypatch.delattr("requests.sessions.Session.request")
