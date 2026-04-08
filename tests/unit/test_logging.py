"""Tests for nanio.logging.setup_logging."""

from __future__ import annotations

import logging

import pytest

from nanio.logging import setup_logging


def test_setup_logging_sets_root_level():
    setup_logging("debug")
    assert logging.getLogger().level == logging.DEBUG
    setup_logging("warning")
    assert logging.getLogger().level == logging.WARNING


def test_setup_logging_is_idempotent():
    setup_logging("info")
    n_before = sum(getattr(h, "_nanio", False) for h in logging.getLogger().handlers)
    setup_logging("info")
    n_after = sum(getattr(h, "_nanio", False) for h in logging.getLogger().handlers)
    assert n_before == n_after == 1


def test_setup_logging_unknown_level():
    with pytest.raises(ValueError, match="unknown log level"):
        setup_logging("verbose")
