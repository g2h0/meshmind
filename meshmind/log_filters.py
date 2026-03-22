"""Centralized log noise suppression for MeshMind.

Add noisy library log patterns to _SUPPRESSED to keep them out of both the
TUI log viewer and the persistent log file.
"""

import logging

# (logger-name prefix, message fragment) — if both match, the record is dropped.
_SUPPRESSED = [
    ("phonemizer", "words count mismatch"),
]


class LibraryNoiseFilter(logging.Filter):
    """Drop known noisy library warnings that clutter logs."""

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        for logger_prefix, fragment in _SUPPRESSED:
            if record.name.startswith(logger_prefix) and fragment in msg:
                return False
        return True
