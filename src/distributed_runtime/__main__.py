"""Module entrypoint: ``python -m distributed_runtime <role>``."""

import sys

from distributed_runtime.deploy.main import main

if __name__ == "__main__":
    sys.exit(main())
