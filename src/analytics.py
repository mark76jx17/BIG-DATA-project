"""
Analytics module for Urban Services Analysis.
Provides statistical analysis and accessibility metrics.
"""

import pandas as pd
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from typing import Dict, List, Optional

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
            'mean_services_per_cell': float(city_data['service_count'].mean()).__round__(2),
            'median_services_per_cell': float(city_data['service_count'].median()).__round__(2),
            'max_services_per_cell': int(city_data['service_count'].max()).__round__(2),
            'min_services_per_cell': int(city_data['service_count'].min()).__round__(2),
            'std_services_per_cell': float(city_data['service_count'].std()).__round__(2)
        }

        # Category breakdown
        category_stats = {}
        categories = [col for col in city_data.columns if col not in ("h3_index", "city", "lat", "lng")]
        for cat in categories:
            if cat in city_data.columns:
                category_stats[cat] = int(city_data[cat].sum())
        stats[city]['categories'] = category_stats

    return pd.DataFrame(stats)


def calculate_accessibility_metrics(df: pd.DataFrame,
                                    threshold: int) -> Dict:
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
            'percentage_poorly_served': (poorly_served / total * 100) if total > 0 else 0,
            'accessibility_empiric_index': city_data["accessibility_index"].mean()
        }

    return results

def compare_cities(df: pd.DataFrame, service_categories: list) -> pd.DataFrame:
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
            'total_services': city_data['service_count'].sum().__round__(2),
            'avg_services_per_cell': city_data['service_count'].mean().__round__(2),
            'median_services': city_data['service_count'].median().__round__(2),
            'max_services': city_data['service_count'].max().__round__(2),
            'service_coverage_std': city_data['service_count'].std().__round__(2)
        }

        # Add category totals
        for cat in service_categories:
            if cat in city_data.columns:
                row[f'{cat}_total'] = city_data[cat].sum()

        comparison.append(row)

    return pd.DataFrame(comparison)


def generate_report(df: pd.DataFrame,
                    stats: Dict,
                    accessibility: Dict,
                    treshold: int) -> str:
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
    lines.append(f"  Threshold: {treshold}+ services = well-served")
    for city, access in accessibility.items():
        lines.append(f"\n  {city}:")
        lines.append(f"    - Well-served cells: {access['well_served_cells']}")
        lines.append(f"    - Poorly-served cells: {access['poorly_served_cells']}")
        lines.append(f"    - Accessibility rate: {access['percentage_well_served']:.1f}%")
        lines.append(f"    - Accessibility Empiric Index: {access['accessibility_empiric_index']:.1f}")

    lines.append("\n" + "=" * 70)

    return "\n".join(lines)


def print_report(df: pd.DataFrame, save_file: str, config: dict) -> None:
    """
    Calculate statistics and print a formatted report.

    Args:
        df: Aggregated DataFrame
    """
    stats = calculate_descriptive_stats(df)
    accessibility = calculate_accessibility_metrics(df, threshold=config.get("WELL_SERVED_THRESHOLD"))
    report = generate_report(df, stats, accessibility, treshold=config.get("WELL_SERVED_THRESHOLD"))
    with open(save_file, "w") as file:
        file.write(report)
    print(report)


if __name__ == "__main__":
    ...
