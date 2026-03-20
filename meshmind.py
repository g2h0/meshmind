#!/usr/bin/env python3
"""MESHMIND v0.3 - TUI Entry Point"""

import sys
import os

# Ensure the project root is in the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from meshmind.app import run_app

if __name__ == "__main__":
    run_app()
