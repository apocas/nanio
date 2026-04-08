"""Allow `python -m nanio` to invoke the CLI."""

import sys

from nanio.cli import main

if __name__ == "__main__":
    sys.exit(main())
