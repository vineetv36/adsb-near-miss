"""
Shared pytest configuration and fixtures.

Ensures the project root is on sys.path so ``from src.xxx import yyy`` works
whether tests are run with ``pytest`` (from the root) or ``python -m pytest``.
"""

import os
import sys

# Make ``import src.*`` work from any working directory
ROOT = os.path.dirname(os.path.dirname(__file__))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)
