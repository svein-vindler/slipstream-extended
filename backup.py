"""Portable entry point for the four offline backup operations."""

from __future__ import annotations

import sys


def main() -> int:
    if sys.version_info < (3, 12):
        print("backup_error: python_3_12_or_newer_required", file=sys.stderr)
        return 2
    try:
        from pipeline.private_backup import main as backup_main
    except ImportError:
        print("backup_error: backup_installation_incomplete", file=sys.stderr)
        print("See docs/BACKUP_QUICKSTART.md for the hash-locked installation.", file=sys.stderr)
        return 2
    return backup_main()


if __name__ == "__main__":
    sys.exit(main())
