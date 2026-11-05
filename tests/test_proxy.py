"""ProxyRotator support: rotation/retire, masking, budget meter, SourceClient integration (no network)."""
import datetime as dt
import os
from unittest import mock

import pytest
import requests

from sources import proxy
from sources.http import SourceClient
from sources.proxy import ProxyBudgetExceeded, ProxyPool, UsageMeter, mask


def test_mask_hides_credentials():
    with mock.patch.dict(os.environ, {"PROXYROTATOR_PASSWORD": "s3cretpw"}):
        s = mask("ProxyError: http://user:s3cretpw@gw.example:8000 failed; raw s3cretpw")
    assert "s3cretpw" not in s and "user:" not in s and "gw.example:8000" in s


def test_rotation_and_retire():
    t = [0.0]
    p = ProxyPool(["http://a:1", "http://b:2"], retire_after=2, cooldown=60, clock=lambda: t[0])
    assert p.current() == (0, "http://a:1")
    p.fail(0)
    assert p.current()[0] == 1 and p.generation == 1
    p.fail(1); p.fail(0)                     # a: 2 fails -> retired
    p.fail(1)                                # b: 2 fails -> retired
    assert p.stats["retired"] == 2
    t[0] = 61
    assert p.current()[0] in (0, 1)


def test_enabled_defaults():
    with mock.patch.dict(os.environ, {}, clear=True):
        assert proxy.enabled_for("redfin_detail") and not proxy.enabled_for("realtor")
    with mock.patch.dict(os.environ, {"REALTOR_USE_PROXYROTATOR": "1", "REDFIN_USE_PROXYROTATOR": "0"}):
        assert proxy.enabled_for("realtor") and not proxy.enabled_for("redfin")


def test_meter_budget_persist_and_reset(tmp_path):
    f = str(tmp_path / "u.json")
    day = [dt.date(2026, 11, 8)]
    m = UsageMeter(f, budget_gb=1e-6, reset=dt.date(2026, 11, 9), flush_bytes=1, today=lambda: day[0])
    m.add(600); m.add(600)
    assert m.exhausted()
    assert UsageMeter(f, 1, dt.date(2026, 11, 9), today=lambda: day[0]).used_gb * 1e9 == 1200
    day[0] = dt.date(2026, 11, 9)            # renewal -> new period
    assert UsageMeter(f, 1, dt.date(2026, 11, 9), today=lambda: day[0]).used_gb == 0


def test_budget_stop_or_direct(tmp_path):
    m = UsageMeter(str(tmp_path / "u.json"), budget_gb=0, reset=dt.date(2026, 11, 9))
    with pytest.raises(ProxyBudgetExceeded):
        ProxyPool(["http://a:1"], meter=m).current()
    assert ProxyPool(["http://a:1"], meter=m, direct_fallback=True).current() == (-1, None)


def _resp(code, body=b"{}"):
    r = requests.Response()
    r.status_code, r._content = code, body
    r.request = requests.Request("GET", "https://x.test/a").prepare()
    return r


def test_client_rotates_on_403_and_masks(tmp_path):
    pool = ProxyPool(["http://u:pw1234@gw:1", "http://u:pw1234@gw:2"],
                     meter=UsageMeter(str(tmp_path / "u.json"), 150, dt.date(2026, 11, 9)))
    with mock.patch.object(proxy, "pool_from_env", return_value=pool), \
         mock.patch("sources.http.pool_from_env", return_value=pool), \
         mock.patch.dict(os.environ, {"PROXYROTATOR_PASSWORD": "pw1234"}):
        c = SourceClient("redfin_detail", min_interval=0, retries=3, sleep=lambda s: None)
        used = []

        def fake(method, url, proxies=None, **kw):
            used.append(proxies["https"])
            if len(used) == 1:
                raise requests.exceptions.ProxyError(f"Cannot connect to proxy {proxies['https']}")
            if len(used) == 2:
                return _resp(403)
            return _resp(200)
        with mock.patch.object(requests.Session, "request", side_effect=fake):
            assert c.request("GET", "https://x.test/a").status_code == 200
    assert used[0] != used[1] and pool.stats["rotations"] == 2 and pool.meter._total > 0


def test_client_error_message_masked(tmp_path, capsys):
    pool = ProxyPool(["http://u:pw1234@gw:1"])
    with mock.patch("sources.http.pool_from_env", return_value=pool), \
         mock.patch.dict(os.environ, {"PROXYROTATOR_PASSWORD": "pw1234"}):
        c = SourceClient("redfin", min_interval=0, retries=2, sleep=lambda s: None)
        err = requests.exceptions.ProxyError("http://u:pw1234@gw:1 refused")
        with mock.patch.object(requests.Session, "request", side_effect=err):
            with pytest.raises(RuntimeError) as ei:
                c.request("GET", "https://x.test/a")
    assert "pw1234" not in str(ei.value) and "pw1234" not in capsys.readouterr().out


def test_realtor_off_by_default():
    with mock.patch("sources.http.pool_from_env", return_value=ProxyPool(["http://gw:1"])), \
         mock.patch.dict(os.environ, {}, clear=True):
        assert SourceClient("realtor").pool is None
