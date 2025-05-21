#!/usr/bin/env python
import os
import pytest
import sys


def run_tests():
    """Run all the tests with pytest."""
    print("Running tests for Tagify Integration...")

    # Add arguments for better output
    args = [
        "--verbose",
        "--color=yes",
        "tests"
    ]

    # Run tests and exit with appropriate code
    exit_code = pytest.main(args)
    sys.exit(exit_code)


if __name__ == "__main__":
    run_tests()
