#!/usr/bin/env python
"""
Quick-start script to run the Urban Services Analysis.
Place in project root for easy execution.
"""

import sys
from pathlib import Path

# Add src to path

src_path = Path(__file__).parent / 'src'
sys.path.insert(0, str(src_path))

from main import run_analysis

if __name__ == "__main__":
    print("Urban Services Analysis - Quick Start")
    print("=" * 50)

    # Check for --cached flag
    use_cached = '--cached' in sys.argv or '-c' in sys.argv

    # Run analysis
    run_analysis(
        use_cached_data=use_cached,
        save_data=True,
        create_maps=True,
        output_dir='output'
    )
