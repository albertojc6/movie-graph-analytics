from pyspark.sql import functions as F
from pyspark.sql import SparkSession
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
hdfs_formatted = "/data/formatted/MovieTweetings"
hdfs_trusted = "/data/trusted/MovieTweetings"

def quality_MovieTweetings(hdfs_client: HDFSClient):
    """
    Improves and analyzes quality of the MovieTweetings datasets (movies, users, ratings).
    Reads from formatted Parquet files in HDFS and writes trusted data back to HDFS.
    """
    # Clean up temporary files from previous runs
    tmp_dir = "/tmp/MovieTweetings"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)

    datasets = [
        {"file": "movies.parquet", "constraints": apply_constraints_movies},
        {"file": "users.parquet", "constraints": apply_constraints_users},
        {"file": "ratings.parquet", "constraints": apply_constraints_ratings}
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
            df = constraints(df)

            # Remove duplicates
            df = df.dropDuplicates()

            # 5. Save to parquet        
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

def apply_constraints_movies(df: F.DataFrame) -> F.DataFrame:
    """
    Apply constraints specific to the movies dataset.
    """
    # movie_id must not be null
    df = df.filter(F.col("movie_id").isNotNull())
    # movie_id must start with 'tt'
    df = df.filter(F.col("movie_id").startswith("tt"))
    # Eliminar la columna movie_year si existe
    if "movie_year" in df.columns:
        df = df.drop("movie_year")
    return df

def apply_constraints_users(df: F.DataFrame) -> F.DataFrame:
    """
    Apply constraints specific to the users dataset.
    """
    # user_id and twitter_id must not be null
    df = df.filter(F.col("user_id").isNotNull() & F.col("twitter_id").isNotNull())

    return df

def apply_constraints_ratings(df: F.DataFrame) -> F.DataFrame:
    """
    Apply constraints specific to the ratings dataset.
    """
    # user_id and movie_id must not be null
    df = df.filter(F.col("user_id").isNotNull() & F.col("movie_id").isNotNull())

    # rating must be between 0 and 10
    df = df.filter((F.col("rating") >= 0) & (F.col("rating") <= 10))

    # rating_timestamp must be in format 'yyyy-MM-dd HH:mm:ss'
    df = df.withColumn(
        "rating_timestamp",
        F.date_format(F.to_timestamp(F.col("rating_timestamp")), "yyyy-MM-dd HH:mm:ss")
    )

    return df