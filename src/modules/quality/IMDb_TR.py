from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import os
import shutil
from pyspark.sql.functions import year, current_date

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
hdfs_formatted = "/data/formatted/IMDb"
hdfs_trusted = "/data/trusted/IMDb"

def quality_IMDb(hdfs_client: HDFSClient):
    """
    Improves and analyzes quality of the IMDb datasets: title.basics, title.crew, name.basics.
    Reads from formatted Parquet files in HDFS and writes trusted data back to HDFS.
    """
    # Clean up temporary files from previous runs
    tmp_dir = "/tmp/IMDb"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)

    datasets = [
        {
            "file": "title_basics.parquet",
            "constraints": apply_constraints_titlebasics
        },
        {
            "file": "title_crew.parquet",
            "constraints": apply_constraints_titlecrew
        },
        {
            "file": "name_basics.parquet",
            "constraints": apply_constraints_namebasics
        }
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
            df.show(5, truncate=False)

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


def apply_constraints_titlebasics(df: F.DataFrame) -> F.DataFrame:
    """
    Constraints for IMDb title.basics.
    """
    current_year = year(current_date())

    df = df.filter (
        (F.col("runtimeMinutes") >= 0) &  # runtimeMinutes must be >= 0
        (F.col("isAdult").isin(False, True)) &   # isAdult must be Boolean
        (F.col("startYear") >= 1800) & (F.col("startYear") <= current_year) &  # Valid startYear
        (F.col("endYear").isNull() |  # Allow NULL endYear OR valid year
            ((F.col("endYear").cast("int") >= 1800) & (F.col("endYear").cast("int") <= current_year) )
         )  
    )  
    
    # remove variable, redundant
    if "originalTitle" in df.columns:
        df = df.drop("originalTitle")
    return df

def apply_constraints_titlecrew(df: F.DataFrame) -> F.DataFrame:
    """
    Constraints for IMDb title.crew (handles array types).
    """
    # tconst must not be null and must start with 'tt'
    df = df.filter(F.col("tconst").isNotNull() & F.col("tconst").startswith("tt"))
    
    # Clean arrays: remove null/empty values and ensure proper formatting
    df = df.withColumn("directors", F.expr("filter(directors, x -> x is not null and x != '')")) \
           .withColumn("writers", F.expr("filter(writers, x -> x is not null and x != '')"))
    
    # Validation conditions for directors
    directors_condition = (
        (F.size(F.col("directors")) == 0) | 
        F.forall(F.col("directors"), lambda x: x.startswith("nm"))
    )
    
    # Validation conditions for writers
    writers_condition = (
        (F.size(F.col("writers")) == 0) | 
        F.forall(F.col("writers"), lambda x: x.startswith("nm"))
    )
    
    # Apply conditions
    df = df.filter(directors_condition & writers_condition)
    
    # Ensure both columns aren't empty
    df = df.filter(~(
        (F.size(F.col("directors")) == 0) & 
        (F.size(F.col("writers")) == 0)
    ))
    
    return df


def apply_constraints_namebasics(df: F.DataFrame) -> F.DataFrame:
    current_year = year(current_date())

    # nconst must not be null and must start with 'nm'
    df = df.filter(F.col("nconst").isNotNull() & F.col("nconst").startswith("nm"))

    # birthYear/deathYear constraints
    df = df.filter(
        (F.col("birthYear").isNull() | ((F.col("birthYear") >= 1800) & (F.col("birthYear") <= current_year))) &
        (F.col("deathYear").isNull() | ((F.col("deathYear") >= 1800) & (F.col("deathYear") <= current_year)))
    )
    df = df.filter(
        F.col("birthYear").isNull() | F.col("deathYear").isNull() | (F.col("birthYear") <= F.col("deathYear"))
    )

    # clean and validate knownForTitles array
    df = df.withColumn("knownForTitles", F.expr("filter(knownForTitles, x -> x is not null and x != '')"))
    known_condition = (
        (F.size(F.col("knownForTitles")) == 0) |
        F.forall(F.col("knownForTitles"), lambda x: x.startswith("tt"))
    )
    df = df.filter(known_condition)

    return df