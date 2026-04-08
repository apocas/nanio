"""Smoke tests proving the package imports and the CLI runs."""

from __future__ import annotations

import subprocess
import sys

import nanio
from nanio.cli import build_parser, main


def test_package_has_version() -> None:
    assert isinstance(nanio.__version__, str)
    assert nanio.__version__  # non-empty


def test_parser_exposes_serve_subcommand() -> None:
    parser = build_parser()
    # We don't assert on private internals; we just confirm parsing works.
    ns = parser.parse_args(["serve"])
    assert ns.command == "serve"


def test_main_with_no_args_prints_help_and_returns_zero(capsys) -> None:
    rc = main([])
    captured = capsys.readouterr()
    assert rc == 0
    assert "nanio" in captured.out
    assert "serve" in captured.out


def test_python_dash_m_nanio_version() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "nanio", "--version"],
        capture_output=True,
        text=True,
        check=True,
    )
    # argparse prints --version to stdout
    assert "nanio" in result.stdout


def test_nanio_serve_help_lists_all_flags() -> None:
    result = subprocess.run(
        [sys.executable, "-m", "nanio", "serve", "--help"],
        capture_output=True,
        text=True,
        check=True,
    )
    out = result.stdout
    for flag in (
        "--data-dir",
        "--host",
        "--port",
        "--workers",
        "--region",
        "--credentials-file",
        "--log-level",
        "--no-access-log",
    ):
        assert flag in out, f"missing flag {flag} in serve --help"


def test_nanio_serve_without_credentials_fails_fast(tmp_path) -> None:
    """`nanio serve` must refuse to start with no credentials configured."""
    env = {
        "PATH": __import__("os").environ.get("PATH", ""),
        "NANIO_DATA_DIR": str(tmp_path),
    }
    result = subprocess.run(
        [sys.executable, "-m", "nanio", "serve", "--port", "0"],
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode != 0
    assert "NANIO_ACCESS_KEY" in result.stderr or "NANIO_SECRET_KEY" in result.stderr
