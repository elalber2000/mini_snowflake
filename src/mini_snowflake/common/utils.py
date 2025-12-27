import os
from datetime import UTC, datetime
from pathlib import Path
import logging
import sys

MSF_PATH = Path(__file__).resolve().parents[1]

def setup_logging() -> None:
    level_name = os.getenv("LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)

    logging.basicConfig(
        level=level,
        stream=sys.stdout,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )


def _atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    os.replace(tmp, path)


def _curr_date() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat()


def _validate_types(spec: tuple[tuple[object, type], ...]) -> None:
    """
    Validate that each value matches its expected type.

    Example:
        validate_types((
            ("asd", str),
            (123, int),
        ))
    """
    for value, expected_type in spec:
        if not isinstance(value, expected_type):
            raise TypeError(
                f"Expected type {expected_type.__name__}, "
                f"got {type(value).__name__} ({value!r})"
            )
