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


def test_mode_defaults():
    with mock.patch.dict(os.environ, {}, clear=True):
        assert proxy.mode_for("redfin_detail") == "hybrid" and proxy.mode_for("realtor") == "hybrid"
        assert proxy.mode_for("homes") == "off"
    with mock.patch.dict(os.environ, {"REALTOR_USE_PROXYROTATOR": "proxy", "REDFIN_USE_PROXYROTATOR": "off"}):
        assert proxy.mode_for("realtor") == "proxy" and proxy.mode_for("redfin") == "off"


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


def test_global_slots_cap_across_processes(tmp_path):
    import subprocess, sys, textwrap
    d = str(tmp_path / "slots")
    a = proxy.GlobalSlots(d, 3)
    held = [a.try_acquire() for _ in range(3)]
    assert all(h is not None for h in held) and a.try_acquire() is None
    probe = textwrap.dedent(f"""
        from sources.proxy import GlobalSlots
        print(GlobalSlots({d!r}, 3).try_acquire() is None)""")
    out = subprocess.run([sys.executable, "-c", probe], capture_output=True, text=True, cwd=os.getcwd())
    assert out.stdout.strip() == "True"            # another process sees all slots taken
    a.release(held[0])
    assert a.try_acquire() is not None
    assert proxy.GlobalSlots(d, 50).n == 10        # never above 10


def test_crashed_process_slot_is_freed(tmp_path):
    import subprocess, sys
    d = str(tmp_path / "slots")
    subprocess.run([sys.executable, "-c",
                    f"from sources.proxy import GlobalSlots; import os; GlobalSlots({d!r}, 1).try_acquire(); os._exit(1)"],
                   cwd=os.getcwd())
    assert proxy.GlobalSlots(d, 1).try_acquire() is not None


def _hybrid_client(pool, sem_free=True):
    with mock.patch("sources.http.pool_from_env", return_value=pool), mock.patch.dict(os.environ, {}, clear=True):
        return SourceClient("redfin", min_interval=0, retries=3, sleep=lambda s: None)


def test_hybrid_uses_direct_when_proxy_slots_full_then_shifts_on_403():
    pool = ProxyPool(["http://gw:1"], max_threads=1)
    c = _hybrid_client(pool)
    assert c.mode == "hybrid"
    tok = pool.slot()                                # proxy pool busy -> direct
    seen = []

    def fake(method, url, proxies=None, **kw):
        seen.append(proxies)
        if len(seen) == 1:
            pool.release_slot(tok)                   # proxy frees up while direct is blocked
            return _resp(403)
        return _resp(200)
    with mock.patch.object(requests.Session, "request", side_effect=fake):
        assert c.request("GET", "https://x.test/a").status_code == 200
    assert seen[0] is None and seen[1]["https"] == "http://gw:1"
    assert c.stats["shifted_to_proxy"] == 1 and c._direct_until > 0


def test_hybrid_over_budget_goes_direct(tmp_path):
    m = UsageMeter(str(tmp_path / "u.json"), budget_gb=0, reset=dt.date(2026, 11, 9))
    c = _hybrid_client(ProxyPool(["http://gw:1"], meter=m))
    with mock.patch.object(requests.Session, "request", return_value=_resp(200)) as rq:
        c.request("GET", "https://x.test/a")
    assert rq.call_args.kwargs["proxies"] is None


def test_off_mode_never_proxies():
    with mock.patch("sources.http.pool_from_env", return_value=ProxyPool(["http://gw:1"])), \
         mock.patch.dict(os.environ, {"REALTOR_USE_PROXYROTATOR": "off"}, clear=True):
        assert SourceClient("realtor").pool is None


def test_daily_cap_resets_at_midnight(tmp_path):
    day = [dt.date(2026, 11, 6)]
    m = UsageMeter(str(tmp_path / "u.json"), budget_gb=150, reset=dt.date(2026, 11, 9), daily_gb=1e-6,
                   flush_bytes=1, today=lambda: day[0])
    pool = ProxyPool(["http://gw:1"], meter=m)
    m.add(1500)
    assert m.exhausted()
    with pytest.raises(ProxyBudgetExceeded):
        pool.current()                                   # proxy mode skips
    c = _hybrid_client(pool)
    assert c._route() == (None, -1, None)                # hybrid -> direct only
    day[0] = dt.date(2026, 11, 7)                        # local midnight
    assert not m.exhausted() and m.used_gb * 1e9 == 1500 and m.day_gb == 0
    assert pool.current()[0] == 0


def test_env_file_loads_only_proxy_vars(tmp_path):
    f = tmp_path / ".env"
    f.write_text('DATABASE_URL=postgres://x\nPROXYROTATOR_GATEWAY="gw.test:11000"\nexport REDFIN_USE_PROXYROTATOR=hybrid\n')
    with mock.patch.dict(os.environ, {}, clear=True):
        proxy.load_env_file(str(f))
        assert os.environ["PROXYROTATOR_GATEWAY"] == "gw.test:11000"
        assert os.environ["REDFIN_USE_PROXYROTATOR"] == "hybrid" and "DATABASE_URL" not in os.environ


def test_detail_client_tuple_timeout_and_hybrid():
    with mock.patch("sources.http.pool_from_env", return_value=ProxyPool(["http://gw:1"])), \
         mock.patch.dict(os.environ, {}, clear=True):
        c = SourceClient("redfin_detail", timeout=(10, 20))
    assert c.timeout == (10.0, 20.0) and c.mode == "hybrid"
