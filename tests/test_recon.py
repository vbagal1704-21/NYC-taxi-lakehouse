"""Unit tests for reconciliation comparison logic."""

from taxi_lakehouse.quality.recon import _result


def test_exact_match_passes():
    r = _result("rows", "a->b", 100, 100)
    assert r["passed"] and r["difference"] == 0


def test_mismatch_fails():
    r = _result("rows", "a->b", 100, 99)
    assert not r["passed"] and r["difference"] == 1


def test_tolerance_allows_float_noise():
    r = _result("amount", "a->b", 1000.004, 1000.0, tolerance=0.01)
    assert r["passed"]


def test_tolerance_still_catches_real_gaps():
    r = _result("amount", "a->b", 1000.0, 990.0, tolerance=0.01)
    assert not r["passed"]
