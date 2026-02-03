"""
Main entry point for Urban Services Analysis.
Orchestrates the complete analysis pipeline using PySpark.
"""

import os
import sys
import argparse
from pathlib import Path

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from config import LOCATIONS, H3_RESOLUTION, WELL_SERVED_THRESHOLD
from data_loader import download_all_pois, save_raw_data, load_raw_data
from spark_processor import create_spark_session, process_pois_with_spark
from analytics import (
    calculate_descriptive_stats,
    calculate_accessibility_metrics,
    print_report,
    compare_cities
)
from visualization import create_3d_map, create_split_map, save_map


def setup_directories():
    """Create necessary directories for data and output."""
    Path('data').mkdir(exist_ok=True)
    Path('output').mkdir(exist_ok=True)


def run_analysis(use_cached_data: bool = False,
                 save_data: bool = True,
                 create_maps: bool = True,
                 output_dir: str = 'output') -> None:
    """
    Run the complete urban services analysis pipeline.

    Args:
        use_cached_data: If True, load data from cache instead of downloading
        save_data: If True, save processed data to files
        create_maps: If True, generate HTML map files
        output_dir: Directory for output files
    """
    setup_directories()

    print("=" * 70)
    print(" " * 15 + "URBAN SERVICES ANALYSIS WITH PYSPARK")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  - Cities: {', '.join(LOCATIONS)}")
    print(f"  - H3 Resolution: {H3_RESOLUTION}")
    print(f"  - Well-served threshold: {WELL_SERVED_THRESHOLD}+ services")

    # Step 1: Load or download data
    print("\n" + "=" * 50)
    print("STEP 1: Data Acquisition")
    print("=" * 50)

    cache_file = 'data/raw_pois.parquet'

    if use_cached_data and os.path.exists(cache_file):
        print(f"Loading cached data from {cache_file}...")
        gdf_pois = load_raw_data(cache_file)
    else:
        print("Downloading POI data from OpenStreetMap...")
        gdf_pois = download_all_pois()

        if save_data:
            save_raw_data(gdf_pois, cache_file)

    print(f"\nTotal POIs: {len(gdf_pois)}")
    print(f"Distribution by city:")
    print(gdf_pois['city'].value_counts())

    # Step 2: Process with PySpark
    print("\n" + "=" * 50)
    print("STEP 2: PySpark Processing")
    print("=" * 50)

    spark = create_spark_session()

    try:
        df_aggregated = process_pois_with_spark(spark, gdf_pois)

        # Step 3: Analytics
        print("\n" + "=" * 50)
        print("STEP 3: Analytics")
        print("=" * 50)

        print_report(df_aggregated)

        # City comparison
        comparison = compare_cities(df_aggregated)
        print("\nCity Comparison Table:")
        print(comparison.to_string(index=False))

        # Save processed data
        if save_data:
            output_csv = f'{output_dir}/services_h3_aggregated.csv'
            df_aggregated.to_csv(output_csv, index=False)
            print(f"\nAggregated data saved to: {output_csv}")

            comparison_csv = f'{output_dir}/city_comparison.csv'
            comparison.to_csv(comparison_csv, index=False)
            print(f"Comparison table saved to: {comparison_csv}")

        # Step 4: Visualization
        if create_maps:
            print("\n" + "=" * 50)
            print("STEP 4: Visualization")
            print("=" * 50)

            print("\nCreating 3D interactive map...")
            map_3d = create_3d_map(df_aggregated)
            save_map(map_3d, f'{output_dir}/services_map_3d.html')

            print("\nCreating split comparison map...")
            map_split = create_split_map(df_aggregated)
            save_map(map_split, f'{output_dir}/services_map_split.html')

        print("\n" + "=" * 70)
        print(" " * 20 + "ANALYSIS COMPLETE!")
        print("=" * 70)

        print(f"\nGenerated files in '{output_dir}/':")
        print("  - services_h3_aggregated.csv")
        print("  - city_comparison.csv")
        if create_maps:
            print("  - services_map_3d.html")
            print("  - services_map_split.html")

        return df_aggregated

    finally:
        spark.stop()
        print("\nSparkSession stopped.")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Urban Services Analysis using PySpark and H3'
    )

    parser.add_argument(
        '--cached', '-c',
        action='store_true',
        help='Use cached data instead of downloading'
    )

    parser.add_argument(
        '--no-save',
        action='store_true',
        help='Do not save output files'
    )

    parser.add_argument(
        '--no-maps',
        action='store_true',
        help='Skip map generation'
    )

    parser.add_argument(
        '--output', '-o',
        type=str,
        default='output',
        help='Output directory (default: output)'
    )

    args = parser.parse_args()

    run_analysis(
        use_cached_data=args.cached,
        save_data=not args.no_save,
        create_maps=not args.no_maps,
        output_dir=args.output
    )


if __name__ == "__main__":
    main()
