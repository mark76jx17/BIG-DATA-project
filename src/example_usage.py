"""
Example usage of the Urban Services Analysis modules.
Demonstrates how to use individual components of the analysis pipeline.
"""

import pandas as pd
from pathlib import Path

# Example 1: Using individual modules
print("=" * 60)
print("EXAMPLE 1: Module-by-Module Usage")
print("=" * 60)

# Import modules
from config import LOCATIONS, H3_RESOLUTION, SERVICE_CATEGORIES
from data_loader import download_all_pois
from spark_processor import create_spark_session, process_pois_with_spark
from analytics import calculate_descriptive_stats, calculate_accessibility_metrics
from visualization import create_3d_map, save_map


def example_basic_analysis():
    """Run basic analysis using individual modules."""

    # Step 1: Download data
    print("\n1. Downloading POI data...")
    gdf_pois = download_all_pois(locations=['Pavia, Italy'])  # Single city for speed
    print(f"   Downloaded {len(gdf_pois)} POIs")

    # Step 2: Process with PySpark
    print("\n2. Processing with PySpark...")
    spark = create_spark_session(app_name="ExampleAnalysis", memory="2g")

    try:
        df_aggregated = process_pois_with_spark(spark, gdf_pois)
        print(f"   Created {len(df_aggregated)} H3 cells")

        # Step 3: Calculate statistics
        print("\n3. Calculating statistics...")
        stats = calculate_descriptive_stats(df_aggregated)
        accessibility = calculate_accessibility_metrics(df_aggregated)

        for city, city_stats in stats.items():
            print(f"\n   {city}:")
            print(f"   - Total services: {city_stats['total_services']}")
            print(f"   - Average per cell: {city_stats['mean_services_per_cell']:.2f}")
            print(f"   - Accessibility: {accessibility[city]['percentage_well_served']:.1f}%")

        # Step 4: Create visualization
        print("\n4. Creating visualization...")
        map_3d = create_3d_map(df_aggregated)
        save_map(map_3d, 'output/example_map.html')

        return df_aggregated

    finally:
        spark.stop()


def example_custom_locations():
    """Analyze custom locations."""
    print("\n" + "=" * 60)
    print("EXAMPLE 2: Custom Locations")
    print("=" * 60)

    # Custom locations
    custom_locations = ['Milano, Italy', 'Torino, Italy']

    print(f"Analyzing: {', '.join(custom_locations)}")

    gdf = download_all_pois(locations=custom_locations)
    spark = create_spark_session()

    try:
        df = process_pois_with_spark(spark, gdf)
        return df
    finally:
        spark.stop()


def example_category_analysis():
    """Analyze specific service categories."""
    print("\n" + "=" * 60)
    print("EXAMPLE 3: Category-Specific Analysis")
    print("=" * 60)

    # Download and process
    gdf = download_all_pois(locations=['Pavia, Italy'])
    spark = create_spark_session()

    try:
        df = process_pois_with_spark(spark, gdf)

        # Analyze specific categories
        priority_categories = ['Health', 'Education', 'Food', 'Transportation']

        print("\nPriority Categories Analysis:")
        for cat in priority_categories:
            if cat in df.columns:
                total = df[cat].sum()
                cells_with_service = (df[cat] > 0).sum()
                print(f"  {cat}:")
                print(f"    - Total: {total}")
                print(f"    - Cells with service: {cells_with_service}")
                print(f"    - Coverage: {cells_with_service/len(df)*100:.1f}%")

        return df

    finally:
        spark.stop()


def example_spark_operations():
    """Demonstrate direct Spark operations."""
    print("\n" + "=" * 60)
    print("EXAMPLE 4: Direct Spark Operations")
    print("=" * 60)

    from pyspark.sql import functions as F
    from spark_processor import (
        geopandas_to_spark,
        categorize_services_spark,
        add_h3_indices
    )

    # Download data
    gdf = download_all_pois(locations=['Pavia, Italy'])
    spark = create_spark_session()

    try:
        # Convert to Spark
        spark_df = geopandas_to_spark(spark, gdf)

        # Categorize
        spark_df = categorize_services_spark(spark_df)

        # Add H3 indices
        spark_df = add_h3_indices(spark_df)

        # Custom Spark aggregations
        print("\nCustom Spark Aggregations:")

        # Category distribution
        print("\nCategory Distribution:")
        spark_df.groupBy('category').count().orderBy(F.desc('count')).show(10)

        # H3 cells with most services
        print("\nTop 10 H3 cells by service count:")
        (spark_df.groupBy('h3_index')
         .count()
         .orderBy(F.desc('count'))
         .show(10))

        return spark_df

    finally:
        spark.stop()


if __name__ == "__main__":
    import sys

    print("\nUrban Services Analysis - Example Usage")
    print("=" * 60)

    # Run basic example by default
    if len(sys.argv) == 1:
        example_basic_analysis()
    else:
        example_name = sys.argv[1]
        if example_name == "custom":
            example_custom_locations()
        elif example_name == "category":
            example_category_analysis()
        elif example_name == "spark":
            example_spark_operations()
        else:
            print(f"Unknown example: {example_name}")
            print("Available: custom, category, spark")
