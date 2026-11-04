from unittest import mock

import requests

from sources.http import SourceClient


def test_responses_are_saved_and_replayed(tmp_path, monkeypatch):
    monkeypatch.setenv("LA_REPLAY_DIR", str(tmp_path))
    c = SourceClient("t", min_interval=0)
    live = requests.Response()
    live.status_code, live._content = 200, b'{"page": 1}'
    with mock.patch.object(requests.Session, "request", return_value=live) as send:
        assert c.request("GET", "https://x/s", params={"p": 1}).text == '{"page": 1}'
        assert c.request("GET", "https://x/s", params={"p": 1}).text == '{"page": 1}'  # replayed
        assert send.call_count == 1
        c.request("GET", "https://x/s", params={"p": 2})  # another page is fetched
        assert send.call_count == 2
    assert c.stats["replayed"] == 1


def test_failed_responses_are_not_saved(tmp_path, monkeypatch):
    monkeypatch.setenv("LA_REPLAY_DIR", str(tmp_path))
    c = SourceClient("t", min_interval=0, retries=1)
    bad = requests.Response()
    bad.status_code, bad._content = 404, b""
    with mock.patch.object(requests.Session, "request", return_value=bad):
        c.request("GET", "https://x/s")
    assert not list(tmp_path.rglob("*.gz"))
