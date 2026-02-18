"""
Main entry point for Urban Services Analysis.
Orchestrates the complete analysis pipeline using PySpark.
"""

import os
import sys
from pathlib import Path

import warnings
warnings.filterwarnings("ignore", category=UserWarning, module="keplergl.keplergl") 

# Add src to path for imports
sys.path.insert(0, str(Path(__file__).parent))

from data_loader import download_all_pois, save_raw_data, load_raw_data
from spark_processor import create_spark_session, process_pois_with_spark
from analytics import (
    calculate_descriptive_stats,
    calculate_accessibility_metrics,
    print_report,
    compare_cities
)
from visualization import save_map, save_map_with_es, create_category_map
from elasticsearch_integration import (
    create_es_client, create_index, index_h3_data, print_es_summary
)
import yaml

def setup_directories():
    """Create necessary directories for data and output."""
    Path('data').mkdir(exist_ok=True)
    Path('output').mkdir(exist_ok=True)


def run_analysis(enable_es: bool = True,
                 output_dir: str = 'output',
                 config_path: str = "config.yaml",
                 dataset_path: str = None
                 ) -> None:
    """
    Run the complete urban services analysis pipeline.

    Args:
        use_cached_data: If True, load data from cache instead of downloading
        save_data: If True, save processed data to files
        create_maps: If True, generate HTML map files
        enable_es: If True, index data into Elasticsearch
        output_dir: Directory for output files
    """

    with open(config_path, mode="r") as f:
        config = yaml.safe_load(f)

    setup_directories()

    print("=" * 70)
    print(" " * 15 + "URBAN SERVICES ANALYSIS WITH PYSPARK")
    print("=" * 70)
    print(f"\nConfiguration:")
    print(f"  - Cities: {', '.join([loc['city'] for loc in config.get('LOCATIONS')])}")
    print(f"  - H3 Resolution: {config.get('H3_RESOLUTION')}")
    print(f"  - Well-served threshold: {config.get('WELL_SERVED_THRESHOLD')}+ services")

    # ══════════════════════════════════════════════════
    # STEP 1: Data Acquisition (Acquisizione dati)
    # Si scaricano i POI (Points of Interest) da OpenStreetMap per le città
    # configurate, oppure si caricano da cache (file parquet).
    # Il risultato è un GeoDataFrame con geometrie, tag OSM e città.
    # ══════════════════════════════════════════════════
    print("\n" + "=" * 50)
    print("STEP 1: Data Acquisition")
    print("=" * 50)

    print("Downloading POI data from OpenStreetMap...")
    gdf_pois = download_all_pois(config, config_path)


    print(f"\nTotal POIs: {len(gdf_pois)}")
    print(f"Distribution by city:")
    print(gdf_pois['city'].value_counts())
    print(gdf_pois.info())

    # Anteprima: le prime 5 righe del GeoDataFrame grezzo
    # Ogni riga è un POI con la sua geometria (punto/poligono), tag OSM e città
    print("\n>>> Raw dataset preview (data downloaded by OSM):")
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
        df_aggregated = process_pois_with_spark(spark, gdf_pois, config)

        # Anteprima del risultato finale dello Step 2
        print("\n>>> Result of Step 2 - DataFrame aggregated for H3 cell (Pandas):")
        print(df_aggregated.head(5).to_string())
        print(f"\n   Columns: {list(df_aggregated.columns)}")
        print(f"   Total rows (unique H3 cells): {len(df_aggregated)}")

        # ══════════════════════════════════════════════════
        # STEP 3: Analytics
        # Si calcolano statistiche descrittive (media, mediana, std, max, min)
        # e metriche di accessibilità ("città dei 15 minuti") per ogni città.
        # Una cella è "well-served" se ha >= 3 servizi.
        # ══════════════════════════════════════════════════
        print("\n" + "=" * 50)
        print("STEP 3: Analytics")
        print("=" * 50)

        print_report(df_aggregated, save_file=f"{output_dir}/city_report.txt", config=config)

        # City comparison
        comparison = compare_cities(df_aggregated, service_categories=config.get("SERVICE_CATEGORIES"))
        print("\n>>> Comparative table of cities:")
        print(comparison.to_string(index=False))

        comparison.to_csv(f"{output_dir}/city_comparison.csv")

        # Save processed data

        csv_data_name = "services_h3_aggregated.csv"
        output_csv = f'{output_dir}/{csv_data_name}'
        df_aggregated.to_csv(output_csv, index=False)
        print(f"\nAggregated data saved to: {output_csv}")


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
                es = create_es_client(config.get("ELASTICSEARCH_HOST"))
                create_index(es, index_name=config.get("ELASTICSEARCH_INDEX"))
                index_h3_data(es, df_aggregated, config)
                print_es_summary(es, index_name=config.get("ELASTICSEARCH_INDEX"))
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

        print("\n" + "=" * 50)
        print("STEP 5: Visualization")
        print("=" * 50)

        map_name = "category_map.html"

        print("\nCreating category map...")
        category_map = create_category_map(output_csv, config=config)
        save_map(category_map, f'{output_dir}/{map_name}')

        save_map_with_es(category_map, map_name, config=config, config_path=config_path, csv_data_name=csv_data_name)


        print("\n" + "=" * 70)
        print(" " * 20 + "ANALYSIS COMPLETE!")
        print("=" * 70)

        print(f"\nGenerated files in '{output_dir}/':")
        print("  - services_h3_aggregated.csv")
        print("  - city_comparison.csv")

        print("Run python3 output/launch_explorer.py to run the city explorer!\n")

        return df_aggregated

    finally:
        spark.stop()
        print("\nSparkSession stopped.")
