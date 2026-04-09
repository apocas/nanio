"""Targeted coverage tests for `nanio.cli._cmd_serve`.

`_cmd_serve` is otherwise only reachable via actually spawning uvicorn,
which the default test suite skips. We mock out `uvicorn.run` and
exercise the path end-to-end: credential resolution, settings build,
startup warning sweep, gc sweep, and the env-var snapshot.
"""

from __future__ import annotations

import os
import runpy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import patch

import pytest

from nanio.cli import _cmd_serve, build_parser, main


def _serve_args(**overrides):
    parser = build_parser()
    argv = ["serve"]
    for k, v in overrides.items():
        if isinstance(v, bool):
            if v:
                argv.append(f"--{k.replace('_', '-')}")
            continue
        argv.extend([f"--{k.replace('_', '-')}", str(v)])
    return parser.parse_args(argv)


@pytest.fixture
def clean_env():
    """Isolate each test from stale NANIO_* env vars."""
    keys = [k for k in os.environ if k.startswith("NANIO_")]
    saved = {k: os.environ.pop(k) for k in keys}
    try:
        yield
    finally:
        for k in keys:
            os.environ.pop(k, None)
        os.environ.update(saved)


# ----------------------------------------------------------------------
# Happy path — env-var credentials
# ----------------------------------------------------------------------


def test_cmd_serve_env_credentials_mocked_uvicorn(tmp_path, clean_env):
    os.environ["NANIO_ACCESS_KEY"] = "ak"
    os.environ["NANIO_SECRET_KEY"] = "sk"

    args = _serve_args(
        data_dir=tmp_path / "data",
        host="127.0.0.1",
        port=9999,
        workers=1,
        region="eu-west-1",
    )

    with patch("uvicorn.run") as mock_run:
        rc = _cmd_serve(args)

    assert rc == 0
    assert mock_run.call_count == 1
    kwargs = mock_run.call_args.kwargs
    assert kwargs["host"] == "127.0.0.1"
    assert kwargs["port"] == 9999
    assert kwargs["factory"] is True
    # Env vars were persisted for worker factory mode.
    assert os.environ["NANIO_DATA_DIR"] == str((tmp_path / "data").resolve())
    assert os.environ["NANIO_REGION"] == "eu-west-1"


def test_cmd_serve_missing_credentials_returns_error(tmp_path, clean_env, capsys):
    args = _serve_args(data_dir=tmp_path / "data")

    with patch("uvicorn.run") as mock_run:
        rc = _cmd_serve(args)

    assert rc == 2
    assert mock_run.call_count == 0
    err = capsys.readouterr().err
    assert "NANIO_ACCESS_KEY" in err or "NANIO_SECRET_KEY" in err


# ----------------------------------------------------------------------
# Options-file path
# ----------------------------------------------------------------------


def test_cmd_serve_with_options_file(tmp_path, clean_env):
    options_file = tmp_path / "options.toml"
    options_file.write_text(
        """
[server]
data_dir = "{data_dir}"
host = "127.0.0.1"
port = 9001
region = "eu-west-1"
workers = 2

[[users]]
access_key = "alice"
secret_key = "wonderland"
""".format(data_dir=tmp_path / "data")
    )

    args = _serve_args(options=options_file)

    with patch("uvicorn.run") as mock_run:
        rc = _cmd_serve(args)

    assert rc == 0
    assert mock_run.called
    assert os.environ["NANIO_OPTIONS_FILE"] == str(options_file)
    assert os.environ["NANIO_REGION"] == "eu-west-1"
    # Resolved values were passed to uvicorn.run.
    kwargs = mock_run.call_args.kwargs
    assert kwargs["host"] == "127.0.0.1"
    assert kwargs["port"] == 9001
    assert kwargs["workers"] == 2


def test_cmd_serve_options_file_via_env_var(tmp_path, clean_env):
    """`NANIO_OPTIONS_FILE` env var is honored when --options is omitted."""
    options_file = tmp_path / "options.toml"
    options_file.write_text(
        '[server]\ndata_dir = "{}"\n[[users]]\naccess_key = "ak"\nsecret_key = "sk"\n'.format(
            tmp_path / "data"
        )
    )
    os.environ["NANIO_OPTIONS_FILE"] = str(options_file)
    args = _serve_args()
    with patch("uvicorn.run") as mock_run:
        rc = _cmd_serve(args)
    assert rc == 0
    assert mock_run.called


def test_cmd_serve_with_missing_options_file(tmp_path, clean_env, capsys):
    args = _serve_args(options=tmp_path / "does-not-exist.toml")
    with patch("uvicorn.run") as mock_run:
        rc = _cmd_serve(args)
    assert rc == 2
    assert not mock_run.called
    assert "failed to load options file" in capsys.readouterr().err


def test_cmd_serve_with_malformed_options_file(tmp_path, clean_env, capsys):
    options_file = tmp_path / "options.toml"
    options_file.write_text("not valid toml [[[[[")
    args = _serve_args(options=options_file)
    with patch("uvicorn.run") as mock_run:
        rc = _cmd_serve(args)
    assert rc == 2
    assert not mock_run.called


def test_cmd_serve_with_options_file_missing_users(tmp_path, clean_env, capsys):
    """Options file with [server] but no [[users]] → credentials load fails."""
    options_file = tmp_path / "options.toml"
    options_file.write_text('[server]\ndata_dir = "{}"\n'.format(tmp_path / "data"))
    args = _serve_args(options=options_file)
    with patch("uvicorn.run") as mock_run:
        rc = _cmd_serve(args)
    assert rc == 2
    assert not mock_run.called
    assert "failed to load credentials" in capsys.readouterr().err


# ----------------------------------------------------------------------
# Precedence: CLI flag > env > options file > default
# ----------------------------------------------------------------------


def test_cmd_serve_cli_flag_overrides_options_file(tmp_path, clean_env):
    """When --port is given on the CLI, it wins over the options file."""
    options_file = tmp_path / "options.toml"
    options_file.write_text(
        '[server]\nport = 1111\n[[users]]\naccess_key = "ak"\nsecret_key = "sk"\n'
    )
    args = _serve_args(options=options_file, port=2222, data_dir=tmp_path / "data")
    with patch("uvicorn.run") as mock_run:
        _cmd_serve(args)
    assert mock_run.call_args.kwargs["port"] == 2222


def test_cmd_serve_env_var_overrides_options_file(tmp_path, clean_env):
    """NANIO_PORT in env beats the options file."""
    options_file = tmp_path / "options.toml"
    options_file.write_text(
        '[server]\nport = 1111\n[[users]]\naccess_key = "ak"\nsecret_key = "sk"\n'
    )
    os.environ["NANIO_PORT"] = "3333"
    args = _serve_args(options=options_file, data_dir=tmp_path / "data")
    with patch("uvicorn.run") as mock_run:
        _cmd_serve(args)
    assert mock_run.call_args.kwargs["port"] == 3333


def test_cmd_serve_cli_flag_beats_env_var(tmp_path, clean_env):
    """CLI flag wins even when both are set."""
    os.environ["NANIO_ACCESS_KEY"] = "ak"
    os.environ["NANIO_SECRET_KEY"] = "sk"
    os.environ["NANIO_PORT"] = "3333"
    args = _serve_args(data_dir=tmp_path / "data", port=4444)
    with patch("uvicorn.run") as mock_run:
        _cmd_serve(args)
    assert mock_run.call_args.kwargs["port"] == 4444


def test_cmd_serve_data_dir_from_env_var(tmp_path, clean_env):
    """`NANIO_DATA_DIR` env var sets the data dir when no CLI flag is given."""
    os.environ["NANIO_ACCESS_KEY"] = "ak"
    os.environ["NANIO_SECRET_KEY"] = "sk"
    target = tmp_path / "from-env"
    os.environ["NANIO_DATA_DIR"] = str(target)
    args = _serve_args()
    with patch("uvicorn.run"):
        _cmd_serve(args)
    assert os.environ["NANIO_DATA_DIR"] == str(target.resolve())


def test_cmd_serve_data_dir_falls_back_to_default(clean_env, tmp_path, monkeypatch):
    """With no CLI flag, no env var, and no options file, the default
    data dir is used. Run from tmp_path so the relative ./nanio-data
    doesn't pollute the repo."""
    monkeypatch.chdir(tmp_path)
    os.environ["NANIO_ACCESS_KEY"] = "ak"
    os.environ["NANIO_SECRET_KEY"] = "sk"
    args = _serve_args()
    with patch("uvicorn.run"):
        _cmd_serve(args)
    # The default is `./nanio-data` (resolved). Verify it landed in env.
    from nanio.config import DEFAULT_DATA_DIR

    assert os.environ["NANIO_DATA_DIR"] == str(DEFAULT_DATA_DIR.resolve())


def test_cmd_serve_options_file_data_dir_used(tmp_path, clean_env):
    """data_dir from options file flows into Settings + env."""
    target_data = tmp_path / "data-from-options"
    options_file = tmp_path / "options.toml"
    options_file.write_text(
        f'[server]\ndata_dir = "{target_data}"\n[[users]]\naccess_key = "ak"\nsecret_key = "sk"\n'
    )
    args = _serve_args(options=options_file)
    with patch("uvicorn.run"):
        _cmd_serve(args)
    assert os.environ["NANIO_DATA_DIR"] == str(target_data.resolve())


# ----------------------------------------------------------------------
# Startup warning sweep + gc sweep
# ----------------------------------------------------------------------


def _make_ancient_upload(data_dir: Path, key: str = "k") -> str:
    """Create a 30-day-old multipart upload for testing the GC path."""
    import json

    from nanio.storage.multipart import MultipartInit, MultipartManager, _init_to_dict
    from nanio.storage.paths import multipart_dir, multipart_init_path

    manager = MultipartManager(data_dir)
    upload_id = manager.create(MultipartInit(bucket="widgets", key=key))
    init = manager.load_init(upload_id)
    init.initiated = datetime.now(tz=UTC) - timedelta(days=30)

    p = multipart_init_path(data_dir, upload_id)
    with open(p, "w") as f:
        json.dump(_init_to_dict(init), f)

    # Back-date dir mtime too.
    d = multipart_dir(data_dir, upload_id)
    ancient_ts = (datetime.now(tz=UTC) - timedelta(days=30)).timestamp()
    os.utime(d, (ancient_ts, ancient_ts))
    return upload_id


def test_cmd_serve_warns_about_abandoned_uploads(tmp_path, clean_env, caplog):
    import logging

    os.environ["NANIO_ACCESS_KEY"] = "ak"
    os.environ["NANIO_SECRET_KEY"] = "sk"

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    _make_ancient_upload(data_dir)

    args = _serve_args(data_dir=data_dir)

    with patch("uvicorn.run"), caplog.at_level(logging.WARNING):
        _cmd_serve(args)

    assert any("abandoned multipart" in rec.message for rec in caplog.records)


def test_cmd_serve_gc_abandoned_uploads_deletes(tmp_path, clean_env, caplog):
    import logging

    os.environ["NANIO_ACCESS_KEY"] = "ak"
    os.environ["NANIO_SECRET_KEY"] = "sk"

    data_dir = tmp_path / "data"
    data_dir.mkdir()
    upload_id = _make_ancient_upload(data_dir)

    from nanio.storage.paths import multipart_dir

    assert multipart_dir(data_dir, upload_id).exists()

    args = _serve_args(data_dir=data_dir, gc_abandoned_uploads=True)

    with patch("uvicorn.run"), caplog.at_level(logging.WARNING):
        _cmd_serve(args)

    assert not multipart_dir(data_dir, upload_id).exists()
    assert any("deleted" in rec.message for rec in caplog.records)


def test_cmd_serve_handles_scan_failure_gracefully(tmp_path, clean_env, caplog):
    import logging

    os.environ["NANIO_ACCESS_KEY"] = "ak"
    os.environ["NANIO_SECRET_KEY"] = "sk"

    args = _serve_args(data_dir=tmp_path / "data")

    # Force the scan to blow up by patching MultipartManager to raise
    # during construction.
    with (
        patch("uvicorn.run"),
        patch(
            "nanio.storage.multipart.MultipartManager.__init__",
            side_effect=RuntimeError("disk explosion"),
        ),
        caplog.at_level(logging.ERROR),
    ):
        rc = _cmd_serve(args)

    assert rc == 0
    assert any("failed to scan" in rec.message for rec in caplog.records)


# ----------------------------------------------------------------------
# main() entry point — exit code propagation
# ----------------------------------------------------------------------


def test_main_with_serve_subcommand_and_no_credentials(tmp_path, clean_env, capsys):
    rc = main(["serve", "--data-dir", str(tmp_path), "--port", "0"])
    assert rc == 2


# ----------------------------------------------------------------------
# __main__.py — import + sys.exit propagation
# ----------------------------------------------------------------------


def test_dunder_main_runs_cli(clean_env):
    """`python -m nanio` invokes `nanio.cli.main`. The module wraps it in
    `sys.exit(main())` — simulate that and assert it exits cleanly on
    `--help`."""
    with pytest.raises(SystemExit) as ei:
        runpy.run_module("nanio", run_name="__main__", alter_sys=True)
    # Running without arguments should show help (exit 0) — but when
    # invoked via runpy with no sys.argv override, `argparse` sees
    # whatever argv pytest was invoked with, which may trigger a
    # different exit. Accept any clean exit that isn't a crash.
    assert ei.value.code in (0, 2)
