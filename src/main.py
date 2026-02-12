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

from config import LOCATIONS, H3_RESOLUTION, WELL_SERVED_THRESHOLD, SERVICE_CATEGORIES
from data_loader import download_all_pois, save_raw_data, load_raw_data
from spark_processor import create_spark_session, process_pois_with_spark
from analytics import (
    calculate_descriptive_stats,
    calculate_accessibility_metrics,
    print_report,
    compare_cities
)
from visualization import create_3d_map, create_aggregate_map, save_map, save_map_with_es, create_category_map
from elasticsearch_integration import (
    create_es_client, create_index, index_h3_data, print_es_summary
)


def setup_directories():
    """Create necessary directories for data and output."""
    Path('data').mkdir(exist_ok=True)
    Path('output').mkdir(exist_ok=True)


def run_analysis(use_cached_data: bool = False,
                 save_data: bool = True,
                 create_maps: bool = True,
                 enable_es: bool = True,
                 output_dir: str = 'output') -> None:
    """
    Run the complete urban services analysis pipeline.

    Args:
        use_cached_data: If True, load data from cache instead of downloading
        save_data: If True, save processed data to files
        create_maps: If True, generate HTML map files
        enable_es: If True, index data into Elasticsearch
        output_dir: Directory for output files
    """
    setup_directories()

    print("=" * 70)
    print(" " * 15 + "URBAN SERVICES ANALYSIS WITH PYSPARK")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  - Cities: {', '.join([loc['city'] for loc in LOCATIONS])}")
    print(f"  - H3 Resolution: {H3_RESOLUTION}")
    print(f"  - Well-served threshold: {WELL_SERVED_THRESHOLD}+ services")

    # ══════════════════════════════════════════════════
    # STEP 1: Data Acquisition (Acquisizione dati)
    # Si scaricano i POI (Points of Interest) da OpenStreetMap per le città
    # configurate, oppure si caricano da cache (file parquet).
    # Il risultato è un GeoDataFrame con geometrie, tag OSM e città.
    # ══════════════════════════════════════════════════
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
    print(gdf_pois.info())
    print(f"\nAvailable columns: {list(gdf_pois.columns)}")
    # dim
    print(f"GeoDataFrame shape: {gdf_pois.shape}")

    # Anteprima: le prime 5 righe del GeoDataFrame grezzo
    # Ogni riga è un POI con la sua geometria (punto/poligono), tag OSM e città
    print("\n>>> Anteprima GeoDataFrame grezzo (dati scaricati da OSM):")
    cols_to_show = [c for c in ['geometry', 'city', 'amenity', 'leisure', 'shop', 'healthcare'] if c in gdf_pois.columns]
    print(gdf_pois[cols_to_show].head(5).to_string())

    # ══════════════════════════════════════════════════
    # STEP 2: PySpark Processing
    # Il GeoDataFrame viene convertito in Spark DataFrame, ogni POI viene
    # categorizzato, indicizzato con H3 e aggregato per cella esagonale.
    # (I sotto-passaggi 2.1–2.6 sono dettagliati in spark_processor.py)
    # ══════════════════════════════════════════════════

    print("\n" + "=" * 50)
    print("STEP 2: PySpark Processing")
    print("=" * 50)

    spark = create_spark_session()

    try:
        df_aggregated = process_pois_with_spark(spark, gdf_pois)

        # Anteprima del risultato finale dello Step 2
        print("\n>>> Risultato Step 2 - DataFrame aggregato per cella H3 (Pandas):")
        print(df_aggregated.head(5).to_string())
        print(f"\n   Colonne: {list(df_aggregated.columns)}")
        print(f"   Righe totali (celle H3 uniche): {len(df_aggregated)}")

        # ══════════════════════════════════════════════════
        # STEP 3: Analytics
        # Si calcolano statistiche descrittive (media, mediana, std, max, min)
        # e metriche di accessibilità ("città dei 15 minuti") per ogni città.
        # Una cella è "well-served" se ha >= 3 servizi.
        # ══════════════════════════════════════════════════
        print("\n" + "=" * 50)
        print("STEP 3: Analytics")
        print("=" * 50)

        print_report(df_aggregated)

        # City comparison
        comparison = compare_cities(df_aggregated)
        print("\n>>> Tabella comparativa tra città:")
        print(comparison.to_string(index=False))

        # Save processed data
        if save_data:
            output_csv = f'{output_dir}/services_h3_aggregated.csv'
            df_aggregated.to_csv(output_csv, index=False)
            print(f"\nAggregated data saved to: {output_csv}")

            comparison_csv = f'{output_dir}/city_comparison.csv'
            comparison.to_csv(comparison_csv, index=False)
            print(f"Comparison table saved to: {comparison_csv}")

        # ══════════════════════════════════════════════════
        # STEP 4: Elasticsearch Indexing (opzionale)
        # I dati aggregati vengono indicizzati in Elasticsearch per abilitare
        # ricerche rapide per città, categoria e numero minimo di servizi.
        # Ogni documento ES corrisponde a una cella H3.
        # ══════════════════════════════════════════════════
        if enable_es:
            print("\n" + "=" * 50)
            print("STEP 4: Elasticsearch Indexing")
            print("=" * 50)
            try:
                es = create_es_client()
                create_index(es)
                index_h3_data(es, df_aggregated)
                print_es_summary(es)
            except Exception as e:
                print(f"\n  Elasticsearch not available: {e}")
                print("  Skipping ES indexing. Pipeline continues.")

        # ══════════════════════════════════════════════════
        # STEP 5: Visualization
        # Si generano mappe interattive HTML con Kepler.gl:
        # - Mappa 3D: altezza esagoni = densità servizi (service_count)
        # - Mappa split: confronto visivo tra città
        # - Mappa categorie: un layer per ogni tipo di servizio (toggle on/off)
        # ══════════════════════════════════════════════════
        if create_maps:
            print("\n" + "=" * 50)
            print("STEP 5: Visualization")
            print("=" * 50)

            print("\nCreating 3D interactive map...")
            map_3d = create_3d_map(df_aggregated)
            save_map(map_3d, f'{output_dir}/services_map_3d.html')

            if enable_es:
                print("\nCreating ES search page...")
                save_map_with_es(map_3d, f'{output_dir}/services_map_3d.html')

            print("\nCreating split comparison map...")
            map_split = create_aggregate_map(df_aggregated)
            save_map(map_split, f'{output_dir}/services_map_split.html')

            print("\nCreating category map...")
            map_split = create_category_map(df_aggregated, categories=SERVICE_CATEGORIES)
            save_map(map_split, f'{output_dir}/category_map.html')


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
        '--no-es',
        action='store_true',
        help='Skip Elasticsearch indexing'
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
        enable_es=not args.no_es,
        output_dir=args.output
    )


if __name__ == "__main__":
    main()
