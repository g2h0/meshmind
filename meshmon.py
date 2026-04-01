#!/usr/bin/env python3
"""MESHMON v0.1 - Service Status Monitor TUI"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from meshmon.app import run_app

if __name__ == "__main__":
    run_app()
