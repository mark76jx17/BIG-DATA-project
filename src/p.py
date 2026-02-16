import pandas as pd
from keplergl import KeplerGl
import csv

# ====== CONFIG ======
CSV_PATH = "./output/services_h3_aggregated.csv"
OUTPUT_HTML = "mappa.html"
# ====================

# CONFIG BASE KEPLER H3 3D
config_base = {
    "version": "v1",
    "config": {
        "visState": {
            "layers": [
                {
                    "id": "h3_layer",
                    "type": "hexagonId",
                    "config": {
                        "dataId": "dataset_csv",
                        "label": "H3 Layer",
                        "columns": {"hex_id": "h3_index"},
                        "isVisible": True,
                        "visConfig": {
                            "opacity": 0.9,
                            "coverage": 1,
                            "enable3d": True,
                            "elevationScale": 50,                            # ← sizeRange sarà impostato dinamicamente
                        }
                    },
                    "visualChannels": {
                        "colorField": {"name": "service_count", "type": "integer"},
                        "colorScale": "quantile",
                        "sizeField": {"name": "service_count", "type": "integer"},
                        "sizeScale": "linear"
                    }
                }
            ]
        },
        "mapState": {
            "pitch": 60,       # vista 3D inclinata
            "bearing": 0
        },
    }
}


# ---- rileva separatore automaticamente ----
with open(CSV_PATH, "r", encoding="utf-8", errors="replace") as f:
    sample = f.read()
# ---- crea mappa ----
mappa = KeplerGl(height=600, config=config_base)

# ---- aggiunge dataframe (NON stringa CSV) ----
mappa.add_data(data=sample, name="dataset_csv")

# ---- salva html ----
mappa.save_to_html(file_name=OUTPUT_HTML)

print(f"HTML salvato in: {OUTPUT_HTML}")
