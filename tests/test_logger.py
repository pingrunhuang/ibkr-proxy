import os

from src import logger as logger_module


def test_startup_log_path_contains_process_startup_timestamp(monkeypatch, tmp_path):
    monkeypatch.setattr(logger_module, "LOG_DIR", str(tmp_path))
    monkeypatch.setattr(logger_module, "STARTUP_TIMESTAMP", "2026-06-12_09-30-45_123456")

    assert logger_module.startup_log_path() == (
        tmp_path / "ibkr_proxy.2026-06-12_09-30-45_123456.log"
    )


def test_remove_old_startup_logs_keeps_space_for_current_run(monkeypatch, tmp_path):
    monkeypatch.setattr(logger_module, "LOG_DIR", str(tmp_path))
    paths = [
        tmp_path / "ibkr_proxy.2026-06-10_09-00-00_000000.log",
        tmp_path / "ibkr_proxy.2026-06-11_09-00-00_000000.log",
        tmp_path / "ibkr_proxy.2026-06-12_09-00-00_000000.log",
    ]
    for index, path in enumerate(paths):
        path.touch()
        path_stat_time = 1_000 + index
        os.utime(path, (path_stat_time, path_stat_time))

    logger_module.remove_old_startup_logs(backup_count=3)

    assert not paths[0].exists()
    assert paths[1].exists()
    assert paths[2].exists()
