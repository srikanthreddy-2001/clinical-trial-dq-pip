"""Single repeatable test command: `python run_tests.py`.

Wraps pytest so the suite runs identically in the Databricks workspace
(/Workspace rejects __pycache__ and .pytest_cache, so both are disabled),
locally, and in CI (CI calls pytest directly).
"""

import os
import sys

sys.dont_write_bytecode = True  # /Workspace rejects __pycache__ directories
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

import pytest  # noqa: E402  (repo path must be on sys.path first)

if __name__ == "__main__":
    sys.exit(
        pytest.main(
            [os.path.join(ROOT, "tests"), "-v", "--no-header", "-p", "no:cacheprovider"]
        )
    )
