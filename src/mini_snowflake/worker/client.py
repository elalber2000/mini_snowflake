# worker/client.py

from __future__ import annotations

import time

import requests

from .config import WorkerConfig


def register_once_with_retries(cfg: WorkerConfig) -> None:
    payload = {"worker_id": cfg.worker_id, "base_url": cfg.base_url, "load": 0.0}

    while True:
        try:
            r = requests.post(
                f"{cfg.orchestrator_url}/workers/register", json=payload, timeout=5
            )
            if r.status_code < 400:
                return
        except Exception:
            pass
        time.sleep(1)


def heartbeat_forever(cfg: WorkerConfig) -> None:
    while True:
        payload = {"worker_id": cfg.worker_id, "load": 0.0}

        try:
            r = requests.post(
                f"{cfg.orchestrator_url}/workers/heartbeat", json=payload, timeout=5
            )

            if r.status_code == 404:
                register_once_with_retries(cfg)

        except Exception:
            pass

        time.sleep(cfg.heartbeat_seconds)


def registration_and_heartbeat_loop(cfg: WorkerConfig) -> None:
    register_once_with_retries(cfg)
    heartbeat_forever(cfg)
