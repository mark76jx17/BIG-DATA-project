"""
Visualization module for Urban Services Analysis.
Handles KeplerGL map creation and configuration.
"""

import pandas as pd
from keplergl import KeplerGl
from typing import Dict, Optional, List

from config import SERVICE_CATEGORIES, KEPLER_COLOR_RANGE, CATEGORY_COLORS, PALETTES, LOCATIONS

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
                    'border': True,
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


def get_kepler_aggregate_config(center_lat: float = 43.7,
                                center_lng: float = 9.5,
                                zoom: float = 7,
                                categories_info: dict|None=None,
                            ) -> Dict:
    """
    Get KeplerGL configuration for split map comparison.

    Args:
        center_lat: Map center latitude
        center_lng: Map center longitude
        zoom: Initial zoom level

    Returns:
        KeplerGL split map configuration dictionary
    """

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
                # 'layers': [
                #     {
                #         'id': 'h3-split',
                #         'type': 'hexagonId',
                #         'config': {
                #             'dataId': 'services',
                #             'label': 'Services',
                #             'color': [18, 147, 154],
                #             'columns': {'hex_id': 'h3_index'},
                #             'isVisible': True,
                #             'visConfig': {
                #                 'opacity': 0.8,
                #                 'colorRange': KEPLER_COLOR_RANGE,
                #                 'coverage': 1,
                #                 'enable3d': True,
                #                 'elevationScale': 5,
                #                 'sizeRange': [0, 500]
                #             }
                #         },
                #         'visualChannels': {
                #             'colorField': {'name': 'service_count', 'type': 'integer'},
                #             'colorScale': 'linear',
                #             'sizeField': {'name': 'service_count', 'type': 'integer'},
                #             'sizeScale': 'linear',
                #             'opacityField': {'name': 'service_count', 'type': 'integer'},
                #             'opacityScale': 'linear'
                #         }
                #     }
                # ],
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
                }
            },
            'mapState': {
                'dragRotate': True,
                'bearing': 0,
                'latitude': center_lat,
                'longitude': center_lng,
                'pitch': 40,
                'zoom': zoom,
                'isSplit': False
            }
        }
    }

    if categories_info != None:
        config['config']['visState']['layers'] = []

        for i, cat in enumerate(categories_info.keys()):
            field_name = "service_count" if cat == "Total" else cat
            palette = KEPLER_COLOR_RANGE if cat == "Total" else PALETTES[cat]
            new_layer = {
                    'id': f'layer-{cat.lower().replace(" ", "-")}',
                    'type': 'hexagonId',
                    'config': {
                        'dataId': 'services',
                        'label': f'{cat}',
                        'color': PALETTES[cat][4], # Puoi variare il colore qui se vuoi
                        'columns': {'hex_id': 'h3_index'},
                        'isVisible': i == 0,  # Solo il primo layer della lista sarà attivo all'avvio
                        'visConfig': {
                            'opacity': 0.8,
                            "colorRange": {
                                'name': f'Custom {cat}',
                                'type': 'sequential',
                                'category': 'Custom',
                                'colors': PALETTES[cat]
                            },
                            'enable3d': True,
                            'elevationScale': 5,
                            'sizeRange': [0, 500]
                        }
                    },
                    'visualChannels': {
                        'colorField': {'name': field_name, 'type': 'integer'},
                        'colorScale': 'linear',
                        'sizeField': {'name': field_name, 'type': 'integer'},
                        'sizeScale': 'linear',
                        'opacityField': {'name': field_name, 'type': 'integer'},
                        'opacityScale': 'linear'
                    }
                }
            config['config']['visState']['layers'].append(new_layer)


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

    map_3d = KeplerGl(config=config)
    map_3d.add_data(data=df, name='services')

    print("3D map created!")
    print("\nMap controls:")
    print("  - Hold Shift + drag: rotate view")
    print("  - Scroll: zoom")
    print("  - Drag: move map")

    return map_3d


def create_aggregate_map(df: pd.DataFrame,
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
        config = get_kepler_aggregate_config(center_lat, center_lng)

    print("testa", df.head())

    map_split = KeplerGl(config=config)
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
                        categories: str,
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
    # if category not in df.columns:
    #     raise ValueError(f"Category '{category}' not found in data")

    center_lat = df['lat'].mean()
    center_lng = df['lng'].mean()

    config = get_kepler_aggregate_config(center_lat, center_lng, categories_info=PALETTES)

    map_cat = KeplerGl(config=config)
    map_cat.add_data(data=df, name='services')

    print(f"Map for category created!")

    return map_cat


def save_map_with_es(kepler_map: KeplerGl,
                     filename: str,
                     es_host: str = "http://localhost:9200",
                     es_index: str = "urban_services_h3",
                     read_only: bool = False) -> None:
    """
    Create an explorer HTML page with the KeplerGL map (iframe) + ES search panel.
    Also creates a launcher script to serve via HTTP (required for iframes).
    """
    import json
    import os

    output_dir = os.path.dirname(filename)
    # Use the category map which has toggleable layers per service type
    map_basename = 'category_map.html'
    explorer_file = os.path.join(output_dir, 'explorer.html')

    categories_json = json.dumps(SERVICE_CATEGORIES)
    cities_json = json.dumps([loc['city'] for loc in LOCATIONS])

    html = '<!DOCTYPE html>\n<html>\n<head>\n<meta charset="UTF-8">\n'
    html += '<title>Urban Services Explorer</title>\n<style>\n'
    html += '*{margin:0;padding:0;box-sizing:border-box}\n'
    html += 'body{font-family:Segoe UI,sans-serif;background:#0f0f23;color:#e0e0e0;height:100vh;overflow:hidden}\n'
    html += 'html,body{height:100%;width:100%}\n'
    html += '#layout{display:flex;height:100%;width:100%}\n'
    html += '#map-frame{flex:1;border:none;width:100%;height:100%}\n'
    html += '#es-panel{width:400px;min-width:400px;background:#1a1a2e;padding:20px;overflow-y:auto;border-left:2px solid #333;display:flex;flex-direction:column}\n'
    html += '#es-panel h2{font-size:16px;color:#FFC300;border-bottom:1px solid #333;padding-bottom:10px;margin-bottom:14px}\n'
    html += '#es-panel label{display:block;margin:10px 0 4px;font-weight:600;color:#aaa;font-size:11px;text-transform:uppercase;letter-spacing:.5px}\n'
    html += '#es-panel select,#es-panel input[type=number]{width:100%;padding:8px 10px;border-radius:6px;border:1px solid #444;background:#16213e;color:#e0e0e0;font-size:13px}\n'
    html += '#es-panel select:focus,#es-panel input:focus{outline:none;border-color:#FFC300}\n'
    html += '#search-btn{width:100%;margin-top:16px;padding:11px;background:linear-gradient(135deg,#FFC300,#E3611C);color:#1a1a2e;border:none;border-radius:6px;font-weight:700;font-size:14px;cursor:pointer}\n'
    html += '#search-btn:hover{opacity:.85}\n'
    html += '#status{margin-top:12px;padding:8px;border-radius:6px;text-align:center;font-size:12px}\n'
    html += '.ok{background:#0d3320;color:#4ade80}.err{background:#3b1111;color:#f87171}.loading{background:#1e293b;color:#94a3b8}\n'
    html += '.stats{display:flex;flex-wrap:wrap;gap:6px;margin-top:10px}\n'
    html += '.stat{background:#16213e;border-radius:6px;padding:6px 12px;font-size:12px}.stat b{color:#FFC300}\n'
    html += '#table-wrap{flex:1;overflow-y:auto;margin-top:12px;min-height:0}\n'
    html += '#table-wrap table{width:100%;border-collapse:collapse;font-size:11px}\n'
    html += '#table-wrap th{background:#16213e;padding:7px 5px;text-align:left;color:#FFC300;position:sticky;top:0}\n'
    html += '#table-wrap td{padding:5px;border-bottom:1px solid #222}\n'
    html += '#table-wrap tr:hover td{background:#16213e}\n'
    html += '</style>\n</head>\n<body>\n<div id="layout">\n'
    html += '<iframe id="map-frame" src="' + map_basename + '" width="100%" height="100%" style="flex:1;border:none;min-height:100vh" onload="triggerResize(this)"></iframe>\n'
    html += '<div id="es-panel">\n'
    html += '<h2>Elasticsearch Search</h2>\n'
    html += '<label>City</label><select id="f-city"><option value="">All Cities</option></select>\n'
    html += '<label>Service Category</label><select id="f-cat"><option value="">All Categories</option></select>\n'
    html += '<label>Min Services per Cell</label><input type="number" id="f-min" value="0" min="0" max="100">\n'
    html += '<div style="display:flex;gap:8px"><button id="search-btn" onclick="doSearch()" style="flex:1">Search</button>\n'
    html += '<button id="reset-btn" onclick="resetMap()" style="flex:0;padding:11px 16px;background:#333;color:#e0e0e0;border:1px solid #555;border-radius:6px;font-size:13px;cursor:pointer;margin-top:16px">Reset</button></div>\n'
    html += '<div id="status"></div>\n'
    html += '<div class="stats" id="stats"></div>\n'
    html += '<div id="table-wrap"></div>\n'
    html += '</div>\n</div>\n'

    html += '<script>\n'
    html += 'function triggerResize(iframe){setTimeout(function(){try{iframe.contentWindow.dispatchEvent(new Event("resize"))}catch(e){}},500);setTimeout(function(){try{iframe.contentWindow.dispatchEvent(new Event("resize"))}catch(e){}},1500);setTimeout(function(){try{iframe.contentWindow.dispatchEvent(new Event("resize"))}catch(e){}},3000)}\n'
    html += 'var ES="' + es_host + '",IX="' + es_index + '";\n'
    html += 'var CATS=' + categories_json + ',CITIES=' + cities_json + ';\n'
    html += 'var BASE_MAP="' + map_basename + '";\n'
    html += 'CITIES.forEach(function(c){var o=document.createElement("option");o.value=c;o.textContent=c;document.getElementById("f-city").appendChild(o)});\n'
    html += 'CATS.forEach(function(c){var o=document.createElement("option");o.value=c;o.textContent=c;document.getElementById("f-cat").appendChild(o)});\n'
    html += 'function resetMap(){\n'
    html += 'document.getElementById("map-frame").src=BASE_MAP;\n'
    html += 'document.getElementById("f-city").value="";\n'
    html += 'document.getElementById("f-cat").value="";\n'
    html += 'document.getElementById("f-min").value="0";\n'
    html += 'document.getElementById("status").innerHTML="";\n'
    html += 'document.getElementById("status").className="";\n'
    html += 'document.getElementById("stats").innerHTML="";\n'
    html += 'document.getElementById("table-wrap").innerHTML="";\n'
    html += '}\n'
    html += 'async function doSearch(){\n'
    html += 'var city=document.getElementById("f-city").value,cat=document.getElementById("f-cat").value,minS=parseInt(document.getElementById("f-min").value)||0;\n'
    html += 'var st=document.getElementById("status"),ss=document.getElementById("stats"),tw=document.getElementById("table-wrap");\n'
    html += 'st.className="loading";st.textContent="Querying...";ss.innerHTML="";tw.innerHTML="";\n'
    html += 'var mc=[];\n'
    html += 'if(city)mc.push({term:{city:city}});\n'
    html += 'if(cat)mc.push({term:{categories_present:cat}});\n'
    html += 'if(minS>0){if(cat){var r={};r[cat]={gte:minS};mc.push({range:r});}else{mc.push({range:{service_count:{gte:minS}}});}}\n'
    html += 'var q={query:mc.length>0?{bool:{must:mc}}:{match_all:{}},size:10000,sort:[{service_count:"desc"}]};\n'
    html += 'try{\n'
    html += 'var r=await fetch(ES+"/"+IX+"/_search",{method:"POST",headers:{"Content-Type":"application/json"},body:JSON.stringify(q)});\n'
    html += 'if(!r.ok)throw new Error("ES "+r.status);\n'
    html += 'var d=await r.json(),hits=d.hits.hits.map(function(h){return h._source});\n'
    html += 'if(!hits.length){st.className="err";st.textContent="No results.";return}\n'
    html += 'var tot=hits.reduce(function(s,h){return s+(h.service_count||0)},0);\n'
    html += 'var avg=(tot/hits.length).toFixed(1);\n'
    html += 'var mx=Math.max.apply(null,hits.map(function(h){return h.service_count||0}));\n'
    html += 'st.className="ok";st.innerHTML="Found <b>"+hits.length+"</b> cells";\n'
    html += 'var sh="<span class=stat><b>Cells:</b> "+hits.length+"</span>";\n'
    html += 'sh+="<span class=stat><b>Total:</b> "+tot+"</span>";\n'
    html += 'sh+="<span class=stat><b>Avg:</b> "+avg+"</span>";\n'
    html += 'sh+="<span class=stat><b>Max:</b> "+mx+"</span>";\n'
    html += 'if(cat){var ct=hits.reduce(function(s,x){return s+(x[cat]||0)},0);sh+="<span class=stat><b>"+cat+":</b> "+ct+"</span>"}\n'
    html += 'ss.innerHTML=sh;\n'
    html += 'var dc=cat||"Health",top=hits.slice(0,200);\n'
    html += 'var t="<table><tr><th>H3</th><th>City</th><th>Total</th><th>"+dc+"</th><th>Lat</th><th>Lng</th></tr>";\n'
    html += 'top.forEach(function(x){t+="<tr><td>"+x.h3_index+"</td><td>"+x.city+"</td><td>"+x.service_count+"</td><td>"+(x[dc]||0)+"</td><td>"+(x.lat||0).toFixed(4)+"</td><td>"+(x.lng||0).toFixed(4)+"</td></tr>"});\n'
    html += 't+="</table>";\n'
    html += 'if(hits.length>200)t+="<p style=color:#888;font-size:11px;margin-top:6px>Top 200 of "+hits.length+"</p>";\n'
    html += 'tw.innerHTML=t;\n'
    # Generate filtered map via server API
    html += 'var params=new URLSearchParams();\n'
    html += 'if(city)params.set("city",city);\n'
    html += 'if(cat)params.set("category",cat);\n'
    html += 'if(minS>0)params.set("min_services",minS);\n'
    html += 'document.getElementById("map-frame").src="/filter?"+params.toString();\n'
    html += '}catch(e){st.className="err";st.textContent="Error: "+e.message}\n'
    html += '}\n'
    html += '</script>\n</body>\n</html>'

    with open(explorer_file, 'w', encoding='utf-8') as f:
        f.write(html)

    # Create launcher script with /filter endpoint
    launcher_file = os.path.join(output_dir, 'launch_explorer.py')
    launcher = '''#!/usr/bin/env python3
"""Launcher for Urban Services Explorer with ES-filtered map generation."""
import http.server
import os
import sys
import json
import tempfile
from urllib.parse import urlparse, parse_qs

PORT = 8050
os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'src'))

from elasticsearch import Elasticsearch
from config import ELASTICSEARCH_HOST, ELASTICSEARCH_INDEX, SERVICE_CATEGORIES, PALETTES
from keplergl import KeplerGl
import pandas as pd


def query_es(city=None, category=None, min_services=None):
    es = Elasticsearch(ELASTICSEARCH_HOST)
    must = []
    if city:
        must.append({"term": {"city": city}})
    if category:
        must.append({"term": {"categories_present": category}})
    if min_services:
        must.append({"range": {"service_count": {"gte": int(min_services)}}})
    q = {"bool": {"must": must}} if must else {"match_all": {}}
    r = es.search(index=ELASTICSEARCH_INDEX, query=q, size=10000)
    hits = [h["_source"] for h in r["hits"]["hits"]]
    if not hits:
        return None
    df = pd.DataFrame(hits)
    df.drop(columns=["location", "categories_present"], errors="ignore", inplace=True)
    for cat in SERVICE_CATEGORIES:
        if cat in df.columns:
            df[cat] = pd.to_numeric(df[cat], errors="coerce").fillna(0).astype(int)
    df["service_count"] = pd.to_numeric(df["service_count"], errors="coerce").fillna(0).astype(int)
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lng"] = pd.to_numeric(df["lng"], errors="coerce")
    return df


def make_filtered_map(df, category=None):
    from visualization import get_kepler_aggregate_config
    center_lat = df["lat"].mean()
    center_lng = df["lng"].mean()
    if category and category in PALETTES:
        config = get_kepler_aggregate_config(center_lat, center_lng,
                                              categories_info={category: PALETTES[category]})
    else:
        config = get_kepler_aggregate_config(center_lat, center_lng,
                                              categories_info=PALETTES)
    m = KeplerGl(config=config)
    m.add_data(data=df, name="services")
    tmp = os.path.join(tempfile.gettempdir(), "es_filtered_map.html")
    m.save_to_html(file_name=tmp, read_only=False)
    with open(tmp, "rb") as f:
        return f.read()


class Handler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/filter":
            params = parse_qs(parsed.query)
            city = params.get("city", [None])[0]
            category = params.get("category", [None])[0]
            min_s = params.get("min_services", [None])[0]
            try:
                df = query_es(city, category, min_s)
                if df is None or df.empty:
                    self.send_response(200)
                    self.send_header("Content-Type", "text/html")
                    self.end_headers()
                    self.wfile.write(b"<html><body style=background:#0f0f23;color:#fff;display:flex;align-items:center;justify-content:center;height:100vh><h2>No results found</h2></body></html>")
                    return
                html_bytes = make_filtered_map(df, category)
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(html_bytes)
            except Exception as e:
                self.send_response(500)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(f"<html><body style=background:#0f0f23;color:#f87171;display:flex;align-items:center;justify-content:center;height:100vh><h2>Error: {e}</h2></body></html>".encode())
            return
        super().do_GET()

    def log_message(self, format, *args):
        print(f"[server] {args[0]}")


print(f"Urban Services Explorer")
print(f"Open http://localhost:{PORT}/explorer.html in your browser")
print(f"Press Ctrl+C to stop")
http.server.HTTPServer(("", PORT), Handler).serve_forever()
'''

    with open(launcher_file, 'w', encoding='utf-8') as f:
        f.write(launcher)

    print(f"Explorer page saved to: {explorer_file}")
    print(f"Launcher script saved to: {launcher_file}")


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
