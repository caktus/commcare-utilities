import sys
from unittest.mock import MagicMock

import pytest

# Globally mock the redcap module (not installed with tox, so can't be imported)
sys.modules["redcap"] = MagicMock()


@pytest.fixture(autouse=True)
def no_requests(monkeypatch):
    """Remove requests.sessions.Session.request for all tests.

    See (https://docs.pytest.org/en/stable/monkeypatch.html#global-patch-example-
       preventing-requests-from-remote-operations)
    """
    monkeypatch.delattr("requests.sessions.Session.request")
