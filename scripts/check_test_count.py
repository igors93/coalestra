from __future__ import annotations

import argparse

import pytest


class CollectionCounter:
    def __init__(self) -> None:
        self.count = 0

    def pytest_collection_finish(self, session: pytest.Session) -> None:
        self.count = len(session.items)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--minimum", type=int, default=500)
    args = parser.parse_args()

    counter = CollectionCounter()
    exit_code = pytest.main(["--collect-only", "-q"], plugins=[counter])
    if exit_code != pytest.ExitCode.OK:
        return int(exit_code)
    print(f"Collected tests: {counter.count}")
    if counter.count < args.minimum:
        print(f"Test floor not met: expected at least {args.minimum}")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
