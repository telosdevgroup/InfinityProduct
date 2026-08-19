#!/usr/bin/env python3
"""Root forwarder for web/server.py"""
import sys
import os

# Ensure web/ is in sys.path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from web.server import main

if __name__ == "__main__":
    main()
