# conftest.py — pytest configuration
# Ensures the project root is in sys.path so imports work without installing the package.
# conftest.py

import sys
import os

sys.path.insert(0, os.path.dirname(__file__))