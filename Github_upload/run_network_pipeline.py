#!/usr/bin/env python3
from __future__ import annotations

import sys

from pipeline_04_aas_integration_adapter import main


if __name__ == "__main__":
    raise SystemExit(main(["network", *sys.argv[1:]]))
