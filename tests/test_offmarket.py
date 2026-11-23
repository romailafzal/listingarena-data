from jobs.offmarket import body


def test_body_status_and_dates():
    b = body("Hood County, TX", "off_market", 400, dates=("2020-01-01", "2020-06-30"))
    q = b["variables"]["query"]
    assert q["status"] == ["off_market"] and q["sold_date"] == {"min": "2020-01-01", "max": "2020-06-30"}
    assert b["variables"]["offset"] == 400


def test_body_no_dates_has_no_window():
    assert "sold_date" not in body("Hood County, TX", "sold", 0)["variables"]["query"]
