"""
Analytics module for Urban Services Analysis.
Provides statistical analysis and accessibility metrics.
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Optional

from config import WELL_SERVED_THRESHOLD, SERVICE_CATEGORIES


def calculate_descriptive_stats(df: pd.DataFrame) -> Dict:
    """
    Calculate descriptive statistics for the aggregated data.

    Args:
        df: Pandas DataFrame with aggregated H3 data

    Returns:
        Dictionary with statistics per city
    """
    stats = {}

    for city in df['city'].unique():
        city_data = df[df['city'] == city]

        stats[city] = {
            'total_cells': len(city_data),
            'total_services': int(city_data['service_count'].sum()),
            'mean_services_per_cell': float(city_data['service_count'].mean()),
            'median_services_per_cell': float(city_data['service_count'].median()),
            'max_services_per_cell': int(city_data['service_count'].max()),
            'min_services_per_cell': int(city_data['service_count'].min()),
            'std_services_per_cell': float(city_data['service_count'].std())
        }

        # Category breakdown
        category_stats = {}
        for cat in SERVICE_CATEGORIES:
            if cat in city_data.columns:
                category_stats[cat] = int(city_data[cat].sum())
        stats[city]['categories'] = category_stats

    return stats


def calculate_accessibility_metrics(df: pd.DataFrame,
                                    threshold: int = WELL_SERVED_THRESHOLD) -> Dict:
    """
    Analyze accessibility using the 15-minute city concept.

    Args:
        df: Pandas DataFrame with aggregated H3 data
        threshold: Minimum services for a cell to be "well-served"

    Returns:
        Dictionary with accessibility metrics per city
    """
    results = {}

    for city in df['city'].unique():
        city_data = df[df['city'] == city]

        well_served = len(city_data[city_data['service_count'] >= threshold])
        poorly_served = len(city_data[city_data['service_count'] < threshold])
        total = len(city_data)

        results[city] = {
            'well_served_cells': well_served,
            'poorly_served_cells': poorly_served,
            'total_cells': total,
            'percentage_well_served': (well_served / total * 100) if total > 0 else 0,
            'percentage_poorly_served': (poorly_served / total * 100) if total > 0 else 0
        }

    return results


def calculate_accessibility_spark(spark_df: DataFrame,
                                  threshold: int = WELL_SERVED_THRESHOLD) -> Dict:
    """
    Calculate accessibility metrics using PySpark.

    Args:
        spark_df: Spark DataFrame with aggregated data
        threshold: Minimum services threshold

    Returns:
        Dictionary with accessibility metrics
    """
    # Add well_served column
    spark_df = spark_df.withColumn(
        'well_served',
        F.when(F.col('service_count') >= threshold, 1).otherwise(0)
    )

    # Aggregate by city
    results_df = (spark_df.groupBy('city')
                  .agg(
                      F.count('*').alias('total_cells'),
                      F.sum('well_served').alias('well_served_cells'),
                      F.sum('service_count').alias('total_services'),
                      F.avg('service_count').alias('avg_services')
                  ))

    # Convert to dictionary
    results = {}
    for row in results_df.collect():
        city = row['city']
        total = row['total_cells']
        well_served = row['well_served_cells']

        results[city] = {
            'total_cells': total,
            'well_served_cells': well_served,
            'poorly_served_cells': total - well_served,
            'total_services': row['total_services'],
            'avg_services_per_cell': row['avg_services'],
            'percentage_well_served': (well_served / total * 100) if total > 0 else 0
        }

    return results


def identify_service_deserts(df: pd.DataFrame,
                             categories: List[str] = None,
                             threshold: int = 0) -> pd.DataFrame:
    """
    Identify areas lacking specific service categories.

    Args:
        df: Pandas DataFrame with aggregated H3 data
        categories: List of categories to check (None = all essential)
        threshold: Maximum services to be considered a "desert"

    Returns:
        DataFrame with service desert areas
    """
    if categories is None:
        categories = ['Health', 'Education', 'Food', 'Food Retail']

    # Filter to cells where all specified categories are below threshold
    mask = pd.Series([True] * len(df))

    for cat in categories:
        if cat in df.columns:
            mask &= (df[cat] <= threshold)

    deserts = df[mask].copy()
    deserts['missing_categories'] = ', '.join(categories)

    return deserts


def compare_cities(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a comparison table between cities.

    Args:
        df: Pandas DataFrame with aggregated H3 data

    Returns:
        DataFrame with city comparison
    """
    comparison = []

    for city in df['city'].unique():
        city_data = df[df['city'] == city]

        row = {
            'city': city,
            'total_h3_cells': len(city_data),
            'total_services': city_data['service_count'].sum(),
            'avg_services_per_cell': city_data['service_count'].mean(),
            'median_services': city_data['service_count'].median(),
            'max_services': city_data['service_count'].max(),
            'service_coverage_std': city_data['service_count'].std()
        }

        # Add category totals
        for cat in SERVICE_CATEGORIES:
            if cat in city_data.columns:
                row[f'{cat}_total'] = city_data[cat].sum()

        comparison.append(row)

    return pd.DataFrame(comparison)


def generate_report(df: pd.DataFrame,
                    stats: Dict,
                    accessibility: Dict) -> str:
    """
    Generate a text report of the analysis.

    Args:
        df: Aggregated DataFrame
        stats: Descriptive statistics dictionary
        accessibility: Accessibility metrics dictionary

    Returns:
        Formatted report string
    """
    lines = []
    lines.append("=" * 70)
    lines.append(" " * 20 + "URBAN SERVICES ANALYSIS REPORT")
    lines.append("=" * 70)

    lines.append(f"\nDATA SUMMARY:")
    lines.append(f"  Total H3 cells analyzed: {len(df)}")
    lines.append(f"  Cities: {', '.join(df['city'].unique())}")

    lines.append(f"\nCITY COMPARISON:")
    for city, city_stats in stats.items():
        lines.append(f"\n  {city.upper()}:")
        lines.append(f"    - H3 cells with services: {city_stats['total_cells']}")
        lines.append(f"    - Total services: {city_stats['total_services']}")
        lines.append(f"    - Average services/cell: {city_stats['mean_services_per_cell']:.2f}")
        lines.append(f"    - Median services/cell: {city_stats['median_services_per_cell']:.0f}")
        lines.append(f"    - Max services in one cell: {city_stats['max_services_per_cell']}")

    lines.append(f"\nACCESSIBILITY ANALYSIS (15-Minute City Concept):")
    lines.append(f"  Threshold: {WELL_SERVED_THRESHOLD}+ services = well-served")
    for city, access in accessibility.items():
        lines.append(f"\n  {city}:")
        lines.append(f"    - Well-served cells: {access['well_served_cells']}")
        lines.append(f"    - Poorly-served cells: {access['poorly_served_cells']}")
        lines.append(f"    - Accessibility rate: {access['percentage_well_served']:.1f}%")

    lines.append("\n" + "=" * 70)

    return "\n".join(lines)


def print_report(df: pd.DataFrame) -> None:
    """
    Calculate statistics and print a formatted report.

    Args:
        df: Aggregated DataFrame
    """
    stats = calculate_descriptive_stats(df)
    accessibility = calculate_accessibility_metrics(df)
    report = generate_report(df, stats, accessibility)
    print(report)


if __name__ == "__main__":
    # Test with sample data
    sample_data = {
        'h3_index': ['8a1234', '8a1235', '8a2345', '8a2346'],
        'city': ['Pavia', 'Pavia', 'Cagliari', 'Cagliari'],
        'service_count': [5, 2, 8, 1],
        'Health': [2, 0, 3, 0],
        'Education': [1, 1, 2, 0],
        'Food': [2, 1, 3, 1]
    }

    df = pd.DataFrame(sample_data)
    print_report(df)
