"""
Visualization module for Urban Services Analysis.
Handles KeplerGL map creation and configuration.
"""

import pandas as pd
from keplergl import KeplerGl
from typing import Dict, Optional, List

from config import SERVICE_CATEGORIES, KEPLER_COLOR_RANGE


def get_kepler_3d_config(center_lat: float = 43.7,
                         center_lng: float = 9.5,
                         zoom: float = 8) -> Dict:
    """
    Get KeplerGL configuration for 3D hexagon visualization.

    Args:
        center_lat: Map center latitude
        center_lng: Map center longitude
        zoom: Initial zoom level

    Returns:
        KeplerGL configuration dictionary
    """
    # Build tooltip fields
    tooltip_fields = [
        {'name': 'city', 'format': None},
        {'name': 'service_count', 'format': None}
    ]

    for cat in SERVICE_CATEGORIES:
        tooltip_fields.append({'name': cat, 'format': None})

    config = {
        'version': 'v1',
        'config': {
            'visState': {
                'filters': [],
                'layers': [
                    {
                        'id': 'h3-3d-layer',
                        'type': 'hexagonId',
                        'config': {
                            'dataId': 'services',
                            'label': 'Service Density 3D',
                            'color': [255, 203, 153],
                            'columns': {'hex_id': 'h3_index'},
                            'isVisible': True,
                            'visConfig': {
                                'opacity': 0.8,
                                'colorRange': KEPLER_COLOR_RANGE,
                                'coverage': 1,
                                'enable3d': True,
                                'sizeRange': [0, 500],
                                'coverageRange': [0, 1],
                                'elevationScale': 5,
                                'enableElevationZoomFactor': True
                            },
                            'hidden': False,
                            'textLabel': [
                                {
                                    'field': None,
                                    'color': [255, 255, 255],
                                    'size': 18,
                                    'offset': [0, 0],
                                    'anchor': 'start',
                                    'alignment': 'center'
                                }
                            ]
                        },
                        'visualChannels': {
                            'colorField': {'name': 'service_count', 'type': 'integer'},
                            'colorScale': 'quantile',
                            'sizeField': {'name': 'service_count', 'type': 'integer'},
                            'sizeScale': 'linear',
                            'coverageField': None,
                            'coverageScale': 'linear'
                        }
                    }
                ],
                'interactionConfig': {
                    'tooltip': {
                        'fieldsToShow': {'services': tooltip_fields},
                        'compareMode': False,
                        'compareType': 'absolute',
                        'enabled': True
                    },
                    'brush': {'size': 0.5, 'enabled': False},
                    'geocoder': {'enabled': False},
                    'coordinate': {'enabled': False}
                },
                'layerBlending': 'normal',
                'splitMaps': [],
                'animationConfig': {'currentTime': None, 'speed': 1}
            },
            'mapState': {
                'bearing': 24,
                'dragRotate': True,
                'latitude': center_lat,
                'longitude': center_lng,
                'pitch': 50,
                'zoom': zoom,
                'isSplit': False
            },
            'mapStyle': {
                'styleType': 'dark',
                'topLayerGroups': {},
                'visibleLayerGroups': {
                    'label': True,
                    'road': True,
                    'border': False,
                    'building': True,
                    'water': True,
                    'land': True,
                    '3d building': False
                },
                'threeDBuildingColor': [9.665468314072013, 17.18305478057247, 31.1442867897876],
                'mapStyles': {}
            }
        }
    }

    return config


def get_kepler_split_config(center_lat: float = 43.7,
                            center_lng: float = 9.5,
                            zoom: float = 7) -> Dict:
    """
    Get KeplerGL configuration for split map comparison.

    Args:
        center_lat: Map center latitude
        center_lng: Map center longitude
        zoom: Initial zoom level

    Returns:
        KeplerGL split map configuration dictionary
    """
    config = {
        'version': 'v1',
        'config': {
            'visState': {
                'filters': [
                    {
                        'dataId': ['services'],
                        'id': 'city-filter',
                        'name': ['city'],
                        'type': 'multiSelect',
                        'value': [],
                        'enlarged': False,
                        'plotType': 'histogram',
                        'animationWindow': 'free',
                        'yAxis': None,
                        'speed': 1
                    }
                ],
                'layers': [
                    {
                        'id': 'h3-split',
                        'type': 'hexagonId',
                        'config': {
                            'dataId': 'services',
                            'label': 'Services by City',
                            'color': [18, 147, 154],
                            'columns': {'hex_id': 'h3_index'},
                            'isVisible': True,
                            'visConfig': {
                                'opacity': 0.8,
                                'colorRange': KEPLER_COLOR_RANGE,
                                'coverage': 1,
                                'enable3d': True,
                                'sizeRange': [0, 500],
                                'elevationScale': 5
                            }
                        },
                        'visualChannels': {
                            'colorField': {'name': 'service_count', 'type': 'integer'},
                            'colorScale': 'quantile',
                            'sizeField': {'name': 'service_count', 'type': 'integer'},
                            'sizeScale': 'linear'
                        }
                    }
                ],
                'splitMaps': [
                    {'layers': {'h3-split': True}},
                    {'layers': {'h3-split': True}}
                ]
            },
            'mapState': {
                'bearing': 0,
                'latitude': center_lat,
                'longitude': center_lng,
                'pitch': 40,
                'zoom': zoom,
                'isSplit': True
            }
        }
    }

    return config


def create_3d_map(df: pd.DataFrame,
                  height: int = 700,
                  config: Optional[Dict] = None) -> KeplerGl:
    """
    Create a 3D interactive map with KeplerGL.

    Args:
        df: DataFrame with aggregated H3 data
        height: Map height in pixels
        config: Optional custom configuration

    Returns:
        KeplerGl map object
    """
    if config is None:
        # Calculate center from data
        center_lat = df['lat'].mean()
        center_lng = df['lng'].mean()
        config = get_kepler_3d_config(center_lat, center_lng)

    map_3d = KeplerGl(height=height, config=config)
    map_3d.add_data(data=df, name='services')

    print("3D map created!")
    print("\nMap controls:")
    print("  - Hold Shift + drag: rotate view")
    print("  - Scroll: zoom")
    print("  - Drag: move map")

    return map_3d


def create_split_map(df: pd.DataFrame,
                     height: int = 700,
                     config: Optional[Dict] = None) -> KeplerGl:
    """
    Create a split comparison map with KeplerGL.

    Args:
        df: DataFrame with aggregated H3 data
        height: Map height in pixels
        config: Optional custom configuration

    Returns:
        KeplerGl split map object
    """
    if config is None:
        center_lat = df['lat'].mean()
        center_lng = df['lng'].mean()
        config = get_kepler_split_config(center_lat, center_lng)

    map_split = KeplerGl(height=height, config=config)
    map_split.add_data(data=df, name='services')

    print("Split comparison map created!")

    return map_split


def save_map(kepler_map: KeplerGl,
             filename: str,
             read_only: bool = False) -> None:
    """
    Save KeplerGL map to HTML file.

    Args:
        kepler_map: KeplerGl map object
        filename: Output filename
        read_only: If True, hide configuration panel
    """
    kepler_map.save_to_html(file_name=filename, read_only=read_only)
    print(f"Map saved to: {filename}")


def create_category_map(df: pd.DataFrame,
                        category: str,
                        height: int = 700) -> KeplerGl:
    """
    Create a map focused on a specific service category.

    Args:
        df: DataFrame with aggregated H3 data
        category: Service category to visualize
        height: Map height in pixels

    Returns:
        KeplerGl map object
    """
    if category not in df.columns:
        raise ValueError(f"Category '{category}' not found in data")

    center_lat = df['lat'].mean()
    center_lng = df['lng'].mean()

    config = get_kepler_3d_config(center_lat, center_lng)

    # Modify config to use category column
    config['config']['visState']['layers'][0]['visualChannels']['colorField'] = {
        'name': category, 'type': 'integer'
    }
    config['config']['visState']['layers'][0]['visualChannels']['sizeField'] = {
        'name': category, 'type': 'integer'
    }
    config['config']['visState']['layers'][0]['config']['label'] = f'{category} Services'

    map_cat = KeplerGl(height=height, config=config)
    map_cat.add_data(data=df, name='services')

    print(f"Map for '{category}' category created!")

    return map_cat


if __name__ == "__main__":
    # Test with sample data
    sample_data = {
        'h3_index': ['891f1d48127ffff', '891f1d48137ffff'],
        'city': ['Pavia', 'Pavia'],
        'service_count': [5, 3],
        'lat': [45.186, 45.187],
        'lng': [9.155, 9.156],
        'Health': [2, 1],
        'Education': [1, 1],
        'Food': [2, 1]
    }

    df = pd.DataFrame(sample_data)

    # Create map (won't display in terminal)
    map_3d = create_3d_map(df)
    print(f"Map object created: {type(map_3d)}")
