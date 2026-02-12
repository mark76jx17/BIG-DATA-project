"""
Data Loader module for Urban Services Analysis.
Handles downloading POI data from OpenStreetMap.
"""

import osmnx as ox
import pandas as pd
import geopandas as gpd
import warnings
from typing import List, Dict, Optional

from config import TAGS, LOCATIONS

# Suppress warnings
warnings.filterwarnings("ignore", category=UserWarning, module="osmnx")
warnings.filterwarnings("ignore", message="Geometry is in a geographic CRS")


def download_pois_for_location(place: dict, tags: Dict) -> Optional[gpd.GeoDataFrame]:
    """
    Download POIs for a single location from OpenStreetMap.

    Args:
        location: City name with country (e.g., 'Pavia, Italy')
        tags: Dictionary of OSM tags to query

    Returns:
        GeoDataFrame with POIs or None if download fails
    """
    try:
        print(f"Downloading data for {place['city']}, {place['country']}...")

        gdf = ox.features.features_from_place(place, tags)
        gdf['city'] = place["city"]

        return gdf
    except Exception as e:
        print(f"  Error downloading {place['city']}, {place['country']}: {e}")
        return None


def download_all_pois(places: List[str] = LOCATIONS,
                      tags: Dict = TAGS) -> gpd.GeoDataFrame:
    """
    Download POIs for all locations.

    Args:
        locations: List of city names to analyze
        tags: Dictionary of OSM tags to query

    Returns:
        GeoDataFrame with all POIs combined

    Raises:
        Exception: If no data could be downloaded
    """
    all_gdfs = []

    for place in places:
        gdf = download_pois_for_location(place, tags)
        if gdf is not None:
            all_gdfs.append(gdf)

    if not all_gdfs:
        raise Exception("No data downloaded!")

    combined = pd.concat(all_gdfs, ignore_index=True)
    print(f"\nTotal POIs downloaded: {len(combined)}")

    return combined


def save_raw_data(gdf: gpd.GeoDataFrame, filepath: str = 'data/raw_pois.parquet') -> None:
    """
    Save raw POI data to parquet file.

    Args:
        gdf: GeoDataFrame with POI data
        filepath: Output file path
    """
    # Convert geometry to WKT for parquet compatibility
    df = pd.DataFrame(gdf)
    df['geometry_wkt'] = gdf.geometry.to_wkt()
    df = df.drop(columns=['geometry'])
    df.to_parquet(filepath, index=False)
    print(f"Raw data saved to {filepath}")


def load_raw_data(filepath: str = 'data/raw_pois.parquet') -> gpd.GeoDataFrame:
    """
    Load raw POI data from parquet file.

    Args:
        filepath: Input file path

    Returns:
        GeoDataFrame with POI data
    """
    from shapely import wkt

    df = pd.read_parquet(filepath)
    df['geometry'] = df['geometry_wkt'].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry='geometry', crs='EPSG:4326')
    gdf = gdf.drop(columns=['geometry_wkt'])
    print(f"Loaded {len(gdf)} POIs from {filepath}")
    return gdf


if __name__ == "__main__":
    # Test download
    gdf = download_all_pois()
    print(f"\nDistribution by city:")
    print(gdf['city'].value_counts())
