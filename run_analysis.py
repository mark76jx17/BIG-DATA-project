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
import argparse
import yaml

parser = argparse.ArgumentParser(
    description='Urban Services Analysis using PySpark and H3'
)

parser.add_argument(
    '--enable_es',
    type=bool,
    default=True,
    help='Enable elasticsearch to sidebar stats'
)

parser.add_argument(
    '--output_dir', '-o',
    type=str,
    default='output',
    help='Output directory (default: output)'
)

parser.add_argument(
    '--config_path',
    type=str,
    default="config.yaml",
    help='Path of the configuration file'
    )

if __name__ == "__main__":
    args = parser.parse_args()

    with open(args.config_path, 'r') as file:
        data = yaml.safe_load(file)

    LOCATIONS = []
    for i in range(3):
        city = input(f"Enter an italian city [or 'end' to terminate]: \n(city {i+1}/3) ")
        if city == "end": break
        LOCATIONS.append({"city": city.strip().capitalize(), "country": "Italy"})

    H3_RESOLUTION = input("""
H3 resolution selection

Res |   Hexagon Area (m²)      |
----+--------------------------+
0   | 4,357,449,416,078.392     |
1   |   609,788,441,794.134     |
2   |    86,801,780,398.997     |
3   |    12,393,434,655.088     |
4   |     1,770,347,654.491     |
5   |       252,903,858.182     |
6   |        36,129,062.164     |
7   |         5,161,293.360     |
8   |           737,327.598     |
9   |           105,332.513     |
10  |            15,047.502     |
11  |             2,149.643     |
12  |               307.092     |
13  |                43.870     |
14  |                 6.267     |
15  |                 0.895     |

Usually selected from 8 to 10 for urban visualization

Enter h3 resolution: """)


    data["LOCATIONS"] = LOCATIONS
    data["H3_RESOLUTION"] = int(H3_RESOLUTION)


    with open(args.config_path, 'w') as file:
        yaml.dump(data, file, default_flow_style=False, sort_keys=False)


    # Run analysis
    run_analysis(
        enable_es=args.enable_es,
        output_dir=args.output_dir,
        config_path=args.config_path
    )
