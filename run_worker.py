#!/usr/bin/env python3
# run_worker.py
import sys
import os
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from src.underwriter_parser.parser_worker import ParserWorker

if __name__ == "__main__":
    print("🚀 Starting Parser Worker...")
    worker = ParserWorker()
    worker.run()