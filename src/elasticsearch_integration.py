"""
Elasticsearch integration module for Urban Services Analysis.
Indexes H3 cell data and provides search functions by service type.
"""

import pandas as pd
from typing import List, Dict, Optional
from elasticsearch import Elasticsearch, helpers

from config import (
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_INDEX,
    SERVICE_CATEGORIES
)


# Elasticsearch index mapping
INDEX_SETTINGS = {
    "number_of_shards": 1,
    "number_of_replicas": 0
}

INDEX_MAPPINGS = {
    "properties": {
        "h3_index": {"type": "keyword"},
        "city": {"type": "keyword"},
        "lat": {"type": "float"},
        "lng": {"type": "float"},
        "location": {"type": "geo_point"},
        "service_count": {"type": "integer"},
        "categories_present": {"type": "keyword"},
        "Health": {"type": "integer"},
        "Education": {"type": "integer"},
        "Food": {"type": "integer"},
        "Food Retail": {"type": "integer"},
        "Retail": {"type": "integer"},
        "Services": {"type": "integer"},
        "Financial": {"type": "integer"},
        "Public Services": {"type": "integer"},
        "Security": {"type": "integer"},
        "Sports": {"type": "integer"},
        "Recreation": {"type": "integer"},
        "Culture": {"type": "integer"},
        "Religion": {"type": "integer"},
        "Transportation": {"type": "integer"},
        "Tourism": {"type": "integer"},
        "Professional Services": {"type": "integer"},
        "Public Utilities": {"type": "integer"},
    }
}


def create_es_client(host: str = ELASTICSEARCH_HOST) -> Elasticsearch:
    """Create and return an Elasticsearch client."""
    es = Elasticsearch(
        host,
        verify_certs=False,
        ssl_show_warn=False,
        request_timeout=30
    )
    if not es.ping():
        raise ConnectionError(f"Cannot connect to Elasticsearch at {host}")
    print(f"Connected to Elasticsearch at {host}")
    return es


def create_index(es: Elasticsearch, index_name: str = ELASTICSEARCH_INDEX) -> None:
    """Create the ES index with mapping. Deletes existing index if present."""
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted existing index '{index_name}'")

    es.indices.create(index=index_name, settings=INDEX_SETTINGS, mappings=INDEX_MAPPINGS)
    print(f"Created index '{index_name}'")


def _build_document(row: pd.Series) -> Dict:
    """Build an ES document from a DataFrame row."""
    doc = {
        "h3_index": row.get("h3_index", ""),
        "city": row.get("city", ""),
        "lat": float(row.get("lat", 0)),
        "lng": float(row.get("lng", 0)),
        "service_count": int(row.get("service_count", 0)),
    }

    # Add geo_point
    if doc["lat"] and doc["lng"]:
        doc["location"] = {"lat": doc["lat"], "lon": doc["lng"]}

    # Add category counts and build categories_present list
    categories_present = []
    for cat in SERVICE_CATEGORIES:
        count = int(row.get(cat, 0))
        doc[cat] = count
        if count > 0:
            categories_present.append(cat)

    doc["categories_present"] = categories_present
    return doc


def index_h3_data(es: Elasticsearch,
                  df: pd.DataFrame,
                  index_name: str = ELASTICSEARCH_INDEX) -> int:
    """Bulk index H3 aggregated data into Elasticsearch."""
    actions = []
    for _, row in df.iterrows():
        doc = _build_document(row)
        actions.append({
            "_index": index_name,
            "_id": doc["h3_index"],
            "_source": doc
        })

    success, errors = helpers.bulk(es, actions, raise_on_error=False)
    print(f"Indexed {success} documents into '{index_name}'")
    if errors:
        print(f"  Errors: {len(errors)}")
    return success


def search_by_service(es: Elasticsearch,
                      category: str,
                      index_name: str = ELASTICSEARCH_INDEX) -> List[Dict]:
    """Find all H3 cells that have a specific service type."""
    result = es.search(
        index=index_name,
        query={"term": {"categories_present": category}},
        size=10000
    )
    return [hit["_source"] for hit in result["hits"]["hits"]]


def search_by_city_and_service(es: Elasticsearch,
                               city: str,
                               category: str,
                               index_name: str = ELASTICSEARCH_INDEX) -> List[Dict]:
    """Find H3 cells in a specific city with a given service type."""
    result = es.search(
        index=index_name,
        query={
            "bool": {
                "must": [
                    {"term": {"city": city}},
                    {"term": {"categories_present": category}}
                ]
            }
        },
        size=10000
    )
    return [hit["_source"] for hit in result["hits"]["hits"]]


def get_top_cells(es: Elasticsearch,
                  category: str,
                  size: int = 10,
                  index_name: str = ELASTICSEARCH_INDEX) -> List[Dict]:
    """Get top H3 cells by service count for a given category."""
    result = es.search(
        index=index_name,
        query={"range": {category: {"gt": 0}}},
        sort=[{category: {"order": "desc"}}],
        size=size
    )
    return [hit["_source"] for hit in result["hits"]["hits"]]


def search_cells(es: Elasticsearch,
                 city: Optional[str] = None,
                 category: Optional[str] = None,
                 min_services: Optional[int] = None,
                 index_name: str = ELASTICSEARCH_INDEX) -> List[Dict]:
    """Search H3 cells with combinable filters."""
    must_clauses = []

    if city:
        must_clauses.append({"term": {"city": city}})
    if category:
        must_clauses.append({"term": {"categories_present": category}})
    if min_services is not None:
        must_clauses.append({"range": {"service_count": {"gte": min_services}}})

    query = {"bool": {"must": must_clauses}} if must_clauses else {"match_all": {}}

    result = es.search(index=index_name, query=query, size=10000)
    return [hit["_source"] for hit in result["hits"]["hits"]]


def print_es_summary(es: Elasticsearch, index_name: str = ELASTICSEARCH_INDEX) -> None:
    """Print a summary of indexed data with sample queries."""
    count = es.count(index=index_name)["count"]
    print(f"\n  Total documents indexed: {count}")

    for cat in ["Health", "Education"]:
        hits = search_by_service(es, cat, index_name)
        print(f"  H3 cells with {cat}: {len(hits)}")

    top = get_top_cells(es, "Health", size=3, index_name=index_name)
    if top:
        print(f"\n  Top 3 H3 cells by Health services:")
        for cell in top:
            print(f"    {cell['h3_index']} ({cell['city']}): {cell['Health']} health services")
