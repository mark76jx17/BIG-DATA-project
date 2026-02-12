"""
PySpark Processor module for Urban Services Analysis.
Handles data processing, categorization, and H3 spatial indexing using PySpark.
"""

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, IntegerType
)
import h3
import pandas as pd
import geopandas as gpd
from typing import Dict, List, Optional

from config import CATEGORY_MAPPING, H3_RESOLUTION, SERVICE_CATEGORIES


def create_spark_session(app_name: str = "UrbanServicesAnalysis",
                         memory: str = "4g") -> SparkSession:
    """
    Create and configure a SparkSession.

    Args:
        app_name: Name of the Spark application
        memory: Driver memory allocation

    Returns:
        Configured SparkSession
    """
    spark = (SparkSession.builder
             .appName(app_name)
             .config("spark.driver.memory", memory)
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())

    spark.sparkContext.setLogLevel("WARN")
    print(f"SparkSession created: {app_name}")
    return spark


def geopandas_to_spark(spark: SparkSession,
                       gdf: gpd.GeoDataFrame,
                       columns: Optional[List[str]] = None) -> DataFrame:
    """
    Convert GeoDataFrame to Spark DataFrame.

    Args:
        spark: SparkSession
        gdf: GeoDataFrame to convert
        columns: List of columns to include (None = auto-detect relevant columns)

    Returns:
        Spark DataFrame
    """
    # Extract centroid coordinates
    gdf = gdf.copy()
    gdf['lat'] = gdf.geometry.centroid.y
    gdf['lng'] = gdf.geometry.centroid.x

    # Select relevant columns
    if columns is None:
        # Auto-detect relevant columns
        relevant_cols = ['lat', 'lng', 'city']
        tag_cols = ['amenity', 'leisure', 'shop', 'healthcare', 'office', 'tourism']
        for col in tag_cols:
            if col in gdf.columns:
                relevant_cols.append(col)
        columns = relevant_cols

    # Filter to only existing columns
    columns = [c for c in columns if c in gdf.columns]

    # Convert to pandas then to spark
    pdf = gdf[columns].copy()

    # Fill NA with empty string for string columns
    for col in pdf.columns:
        if pdf[col].dtype == 'object':
            pdf[col] = pdf[col].fillna('')

    spark_df = spark.createDataFrame(pdf)
    print(f"Converted {len(pdf)} rows to Spark DataFrame")
    return spark_df


def categorize_services_spark(df: DataFrame,
                              category_mapping: Dict = CATEGORY_MAPPING) -> DataFrame:
    """
    Categorize services using PySpark based on OSM tags.

    Args:
        df: Spark DataFrame with POI data
        category_mapping: Dictionary mapping POI types to categories

    Returns:
        DataFrame with 'category' column added
    """
    # Create a mapping expression using CASE WHEN
    # We check each tag column in order of priority
    tag_columns = ['amenity', 'leisure', 'shop', 'healthcare', 'office', 'tourism']

    # Build the categorization expression
    case_expr = None

    for tag_col in tag_columns:
        if tag_col in df.columns:
            for poi_type, category in category_mapping.items():
                condition = F.col(tag_col) == poi_type
                if case_expr is None:
                    case_expr = F.when(condition, F.lit(category))
                else:
                    case_expr = case_expr.when(condition, F.lit(category))

    if case_expr is not None:
        case_expr = case_expr.otherwise(F.lit('Other'))
        df = df.withColumn('category', case_expr)
    else:
        df = df.withColumn('category', F.lit('Other'))

    return df


def calculate_h3_index_udf():
    """
    Create a UDF for calculating H3 index from lat/lng.

    Returns:
        PySpark UDF
    """
    @F.udf(StringType())
    def h3_index_udf(lat: float, lng: float) -> str:
        if lat is None or lng is None:
            return None
        try:
            return h3.latlng_to_cell(lat, lng, H3_RESOLUTION)
        except:
            return None
    return h3_index_udf


def add_h3_indices(df: DataFrame) -> DataFrame:
    """
    Add H3 spatial index to each POI.

    Args:
        df: Spark DataFrame with lat/lng columns

    Returns:
        DataFrame with h3_index column added
    """
    h3_udf = calculate_h3_index_udf()
    df = df.withColumn('h3_index', h3_udf(F.col('lat'), F.col('lng')))

    # Filter out null h3 indices
    df = df.filter(F.col('h3_index').isNotNull())

    print(f"H3 indices calculated at resolution {H3_RESOLUTION}")
    return df


def aggregate_by_h3(df: DataFrame) -> DataFrame:
    """
    Aggregate POI counts by H3 cell and city.

    Args:
        df: Spark DataFrame with h3_index and category columns

    Returns:
        Aggregated DataFrame with service counts per H3 cell
    """
    # Total count per cell
    df_total = (df.groupBy('h3_index', 'city')
                .count()
                .withColumnRenamed('count', 'service_count'))

    # Count per category using pivot
    df_category = (df.groupBy('h3_index', 'city')
                   .pivot('category')
                   .count()
                   .fillna(0))

    # Join total with category breakdown
    df_final = df_total.join(df_category, on=['h3_index', 'city'], how='left')

    print(f"Aggregated to {df_final.count()} unique H3 cells")
    return df_final


def add_h3_centroids(df: DataFrame) -> DataFrame:
    """
    Add lat/lng coordinates for H3 cell centroids.

    Args:
        df: DataFrame with h3_index column

    Returns:
        DataFrame with lat/lng centroid columns
    """
    @F.udf(DoubleType())
    def h3_lat_udf(h3_index: str) -> float:
        if h3_index is None:
            return None
        try:
            return h3.cell_to_latlng(h3_index)[0]
        except:
            return None

    @F.udf(DoubleType())
    def h3_lng_udf(h3_index: str) -> float:
        if h3_index is None:
            return None
        try:
            return h3.cell_to_latlng(h3_index)[1]
        except:
            return None

    df = df.withColumn('lat', h3_lat_udf(F.col('h3_index')))
    df = df.withColumn('lng', h3_lng_udf(F.col('h3_index')))

    return df


def process_pois_with_spark(spark: SparkSession,
                            gdf: gpd.GeoDataFrame) -> pd.DataFrame:
    """
    Complete processing pipeline using PySpark.

    Args:
        spark: SparkSession
        gdf: GeoDataFrame with raw POI data

    Returns:
        Pandas DataFrame with aggregated H3 data
    """
    print("\n" + "="*50)
    print("Starting PySpark Processing Pipeline")
    print("="*50)

    # Convert to Spark DataFrame
    print("\n1. Converting to Spark DataFrame...")
    spark_df = geopandas_to_spark(spark, gdf)

    # Categorize services
    print("\n2. Categorizing services...")
    spark_df = categorize_services_spark(spark_df)

    # Show category distribution
    print("\nCategory distribution:")
    spark_df.groupBy('category').count().orderBy(F.desc('count')).show(20)

    # Calculate H3 indices
    print("\n3. Calculating H3 indices...")
    spark_df = add_h3_indices(spark_df)

    # Aggregate by H3 cell
    print("\n4. Aggregating by H3 cell...")
    aggregated_df = aggregate_by_h3(spark_df)

    # Add centroids
    print("\n5. Adding H3 centroids...")
    aggregated_df = add_h3_centroids(aggregated_df)

    # Convert to Pandas for visualization
    print("\n6. Converting to Pandas DataFrame...")
    result_pdf = aggregated_df.toPandas()

    print("\n" + "="*50)
    print(f"Processing complete! {len(result_pdf)} H3 cells")
    print("="*50)

    return result_pdf


def get_spark_statistics(spark_df: DataFrame) -> Dict:
    """
    Calculate statistics from Spark DataFrame.

    Args:
        spark_df: Processed Spark DataFrame

    Returns:
        Dictionary with statistics
    """
    stats = {}

    # Total rows
    stats['total_pois'] = spark_df.count()

    # POIs per city
    city_counts = (spark_df.groupBy('city')
                   .count()
                   .toPandas()
                   .set_index('city')['count']
                   .to_dict())
    stats['pois_per_city'] = city_counts

    # Categories
    category_counts = (spark_df.groupBy('category')
                       .count()
                       .toPandas()
                       .set_index('category')['count']
                       .to_dict())
    stats['categories'] = category_counts

    return stats


if __name__ == "__main__":
    # Test with sample data
    spark = create_spark_session()

    # Create sample data
    sample_data = [
        (45.1867, 9.1550, 'Pavia', 'restaurant', '', '', '', '', ''),
        (45.1870, 9.1555, 'Pavia', 'hospital', '', '', '', '', ''),
        (39.2238, 9.1217, 'Cagliari', 'school', '', '', '', '', ''),
    ]

    schema = StructType([
        StructField('lat', DoubleType(), True),
        StructField('lng', DoubleType(), True),
        StructField('city', StringType(), True),
        StructField('amenity', StringType(), True),
        StructField('leisure', StringType(), True),
        StructField('shop', StringType(), True),
        StructField('healthcare', StringType(), True),
        StructField('office', StringType(), True),
        StructField('tourism', StringType(), True),
    ])

    test_df = spark.createDataFrame(sample_data, schema)
    test_df = categorize_services_spark(test_df)
    test_df = add_h3_indices(test_df)

    print("\nTest results:")
    test_df.show()

    spark.stop()
