"""Core package initialisation for :mod:`arbitrage_scanner`.

This module disables the standard :mod:`logging` machinery entirely to avoid
spending CPU cycles on formatting log messages and to prevent the logs from
being written to disk.  The application heavily relies on logging throughout
the codebase, therefore the most reliable way to stop every message is to
disable logging at the package import time.
"""

from __future__ import annotations

import logging

# ``logging.disable`` raises the effective level for *all* loggers globally.
# By setting it above ``CRITICAL`` we silence every logging call without the
# need to touch individual modules or handlers.
logging.disable(logging.CRITICAL + 1)
