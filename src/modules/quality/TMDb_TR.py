from pyspark.sql import functions as F
from pyspark.sql import SparkSession, DataFrame
import os
import shutil

# --- UPDATED IMPORTS ---
from utils.other_utils import setup_logging
from utils.hdfs_utils import HDFSClient
from modules.quality.quality_utils import (
    descriptive_profile,
    print_dataset_profile,
    compute_column_completeness,
    compute_relation_completeness
)

# Configure logging
log = setup_logging(__name__)

# Set the HDFS paths
hdfs_formatted = "/data/formatted/TMDb"
hdfs_trusted = "/data/trusted/TMDb"

def quality_TMDb(hdfs_client: HDFSClient):
    """
    Improves and analyzes quality of the TMDb crewData dataset.
    Reads from formatted Parquet files in HDFS and writes trusted data back to HDFS.
    """
    # Clean up temporary files from previous runs
    tmp_dir = "/tmp/TMDb"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)

    datasets = [
        {"file": "crew_data.parquet", "constraints": apply_constraints_TMDb}
    ]

    for dataset in datasets:
        file = dataset["file"]
        constraints = dataset["constraints"]

        log.info(f"Processing dataset: {file}...")

        # Initialize Spark session
        spark = SparkSession.builder \
            .appName(f"Trusted_{file}") \
            .getOrCreate()

        try:
            # 1. Read the Parquet file from HDFS
            input_path = f"{os.getenv('HDFS_FS_URL')}/{hdfs_formatted}/{file}"
            df = spark.read.parquet(input_path)
            df.show(5)

            # 2. Generate Data Profiles (descriptive statistics)
            log.info(f"Generating Data Profiles for {file}...")
            results = descriptive_profile(df)
            profile_print = print_dataset_profile(results)
            print(f"\nDataset profile for {file}:\n{'=' * 40}\n{profile_print}")

            # 3. Computation of Data Quality Metrics
            log.info(f"Computing Data Quality Metrics for {file}...")
            Q_cm_Att = compute_column_completeness(df)
            output_lines = ["\nColumn completeness report:"]
            output_lines.append(f" {'-' * 36} \n{'Column':<25} | {'Missing (%)':>10} \n{'-' * 36}")
            missing_ratios = {row['column']: row['missing_ratio'] for row in Q_cm_Att.collect()}
            popularity_null_ratio = missing_ratios.get('popularity', 0)
            for row in Q_cm_Att.collect():
                missing_pct = f"{row['missing_ratio'] * 100:.2f}%"
                output_lines.append(f"{row['column']:<25} | {missing_pct:>10} \n{'-' * 36}")
            Q_cm_rel = compute_relation_completeness(df)
            print(f"\nRelation's Completeness (ratio of complete rows): {Q_cm_rel:.4f}")
            print("\n".join(output_lines))
            Q_r = df.count() / df.dropDuplicates().count()
            print(f"\nRelation's Redundancy (ratio of duplicates): {Q_r:.4f}")

            # 4. Apply Constraints
            log.info(f"Applying constraints to {file}...")
            df = constraints(df, popularity_null_ratio=popularity_null_ratio)

            # Remove duplicates
            df = df.dropDuplicates()

            # 5. Save to parquet and analyze storage
            f_parquet = f"{tmp_dir}/{file}"
            df.write.mode("overwrite").parquet(f_parquet)
            
            # Store in HDFS
            hdfs_client.copy_from_local(f_parquet, hdfs_trusted, overwrite=True)
            log.info(f"Transferred {f_parquet} to HDFS")

        except Exception as e:
            log.error(f"Pipeline failed for {file}: {str(e)}", exc_info=True)
            raise
        finally:
            spark.stop()
            log.info("Spark session closed.")


def apply_constraints_TMDb(df: DataFrame, popularity_null_ratio: float = 0.0) -> DataFrame:
    """
    Apply constraints specific to the TMDb dataset.
    """
    # imdb_id and tmdb_id must not be null
    df = df.filter(F.col("imdb_id").isNotNull() & F.col("tmdb_id").isNotNull())

    # imdb_id must start with 'nm'
    df = df.filter(F.col("imdb_id").startswith("nm"))

    # popularity must be >= 0
    df = df.filter(F.col("popularity") >= 0)
    df = df.filter(F.col("known_for_popularity") >= 0)

    # gender must be one of the valid values
    valid_genders = ["Female", "Male", "Non-binary"]
    df = df.filter(F.col("gender").isin(valid_genders))

    # adult must be 'true' or 'false'
    df = df.filter(F.col("adult").isin([True, False]))

    # known_for must start with 'tt'
    df = df.filter(F.col("known_for").startswith("tt"))

    # remove original_name 
    if "original_name" in df.columns:
        df = df.drop("original_name")

    # constraints decided based on the previous exploratory analysis:
    # if the popularity column has a low ratio of nulls (<=10%), we can eliminate outliers
    # by keeping only the values between the 5th and 95th percentiles
    # if the ratio of nulls is higher, we do not remove outliers to avoid losing too much data
    THRESHOLD_NULLS = 0.1
    if popularity_null_ratio <= THRESHOLD_NULLS:
        # Calcular percentiles 5 y 95
        quantiles = df.approxQuantile("popularity", [0.05, 0.95], 0.01)
        lower, upper = quantiles
        df = df.filter((F.col("popularity") >= lower) & (F.col("popularity") <= upper))
    return df