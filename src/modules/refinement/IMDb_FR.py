from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from unidecode import unidecode # type: ignore
import shutil
import os

from utils.hdfs_utils import HDFSClient
from utils.other_utils import setup_logging

# Configure logging
log = setup_logging(__name__)

# Set the data lake path where data is, and posterior formatted dir
hdfs_landing = "/data/landing/IMDb/"
hdfs_formatted = "/data/formatted/IMDb"

def format_IMDb(hdfs_client: HDFSClient):
    """
    Formats IMDb data according to a common relational data model, using HDFS

    Args:
        hdfs_client: object for interacting with HDFS easily.
    """

    # Clean up temporary files from previous runs
    tmp_dir = "/tmp/IMDb"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)

    # Format each dataset from IMDb with its corresponding processing
    format_titleBasics(hdfs_client)
    format_titleCrew(hdfs_client)
    format_nameBasics(hdfs_client)


def value_formatting(df):
    """
    Defines correct format for all string values, so as to gain consistency.
    """
    # Collapse multiple spaces and trim all string columns -> consistency!
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.regexp_replace(F.col(col), r"\s+", " ")))

    # - Homogenize NA indicators
    na_values = ['', 'NA', 'N/A', 'NaN', 'NULL', '\\N']
    for col in string_cols:
        df = df.withColumn(col, F.when(F.col(col).isin(na_values), None).otherwise(F.col(col)))
    
    return df

def format_titleBasics(hdfs_client: HDFSClient):
    """
    Formats title_basics datset from IMDb

    Args:
        hdfs_client: object for interacting with HDFS easily.
    """

    spark = None
    log.info("Formatting the titleBasics dataset from IMDb ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .appName("titleBasicsFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema =  StructType([
            StructField("tconst", StringType(), False),          # PK: alphanumeric unique identifier of the title
            StructField("titleType", StringType(), True),        # the type/format of the title (e.g. movie, tvseries)
            StructField("primaryTitle", StringType(), True),     # the more popular title
            StructField("originalTitle", StringType(), True),    # original title, in the original language
            StructField("isAdult", IntegerType(), True),         # 0: non-adult title; 1: adult title
            StructField("startYear", IntegerType(), True),       # release year of a title
            StructField("endYear", StringType(), True),          # TV Series end year. '\N' for all other title types
            StructField("runtimeMinutes", IntegerType(), True),  # runtime of the title, in minutes
            StructField("genres", StringType(), True) # includes up to three genres associated with the title
        ])

        titleBasics_path = hdfs_landing + "title.basics.tsv.gz"
        df = spark.read.format("csv") \
                        .option("header", True) \
                        .option("sep", "\t") \
                        .schema(schema) \
                        .load(f"{os.getenv('HDFS_FS_URL')}/{titleBasics_path}")
        
        # 2. Variable Formatting
        # - primaryTitle and originalTitle --> homogenize strings
        unidecode_udf = F.udf(lambda s: unidecode(s) if s else None, StringType()) # Remove diacritics/accents (e.g., À -> A, é -> e)
        df = df.withColumn("primaryTitle", unidecode_udf(F.col("primaryTitle")))
        df = df.withColumn("originalTitle", unidecode_udf(F.col("originalTitle")))

        # - isAdult --> convert to Boolean
        df = df.withColumn("isAdult", F.col("isAdult") == 1)

        # - genres --> genre1|genre2|genre3: split into an array column [genre1, genre2, genre3]
        df = df.withColumn("genres", F.split(F.col("genres"), ","))

        # 3. Value Formatting
        df = value_formatting(df)

        # 4. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)
        print(df.count())

        # 5. Save to parquet and analyze storage
        tmp_dir = "/tmp/IMDb"
        os.makedirs(tmp_dir, exist_ok=True)
        
        # Get original file size from HDFS
        original_size = hdfs_client.get_size(titleBasics_path)
        
        f_parquet = f"{tmp_dir}/title_basics.parquet"
        
        # Write with overwrite mode
        df.write.mode("overwrite").parquet(f_parquet)
        
        # Calculate Parquet directory size
        parquet_size = 0
        for dirpath, _, filenames in os.walk(f_parquet):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                parquet_size += os.path.getsize(fp)
        
        # Log size comparison
        log.info(f"Size reduction: {original_size} bytes → {parquet_size} bytes ({(1 - parquet_size/original_size)*100:.1f}% saved)")
        
        # Store in HDFS
        hdfs_client.copy_from_local(f_parquet, hdfs_formatted, overwrite=True)
        log.info(f"Transferred {f_parquet} to HDFS")

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")


def format_titleCrew(hdfs_client: HDFSClient):
    """
    Formats titleCrew datset from IMDb

    Args:
        hdfs_client: object for interacting with HDFS easily.
    """

    spark = None
    log.info("Formatting the titleCrew dataset from IMDb ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .appName("titleCrewFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema = StructType([
            StructField("tconst", StringType(), False),    # PK: alphanumeric unique identifier of the title
            StructField("directors", StringType(), True), # director(s) of the given title
            StructField("writers", StringType(), True)    # writer(s) of the given title
        ])

        titleCrew_path = hdfs_landing + "title.crew.tsv.gz"
        df = spark.read.format("csv") \
                        .option("header", True) \
                        .option("sep", "\t") \
                        .schema(schema) \
                        .load(f"{os.getenv('HDFS_FS_URL')}/{titleCrew_path}")
        
        # 2. Value Formatting
        df = value_formatting(df)

        # 3. Variable Formatting
        # directors, writers -> convert string columns to arrays
        df = df.withColumn("directors", F.split("directors", ",")) \
               .withColumn("writers", F.split("writers", ","))

        # 4. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)
        print(df.count())

        # 5. Save to parquet and analyze storage
        tmp_dir = "/tmp/IMDb"
        os.makedirs(tmp_dir, exist_ok=True)
        
        # Get original file size from HDFS
        original_size = hdfs_client.get_size(titleCrew_path)
        
        f_parquet = f"{tmp_dir}/title_crew.parquet"
        
        # Write with overwrite mode
        df.write.mode("overwrite").parquet(f_parquet)
        
        # Calculate Parquet directory size
        parquet_size = 0
        for dirpath, _, filenames in os.walk(f_parquet):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                parquet_size += os.path.getsize(fp)
        
        # Log size comparison
        log.info(f"Size reduction: {original_size} bytes → {parquet_size} bytes ({(1 - parquet_size/original_size)*100:.1f}% saved)")
        
        # Store in HDFS
        hdfs_client.copy_from_local(f_parquet, hdfs_formatted, overwrite=True)
        log.info(f"Transferred {f_parquet} to HDFS")

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")

def format_nameBasics(hdfs_client: HDFSClient):
    """
    Formats nameBasics datset from IMDb

    Args:
        hdfs_client: object for interacting with HDFS easily.
    """

    spark = None
    log.info("Formatting the nameBasics dataset from IMDb ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .appName("nameBasicsFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema = StructType([
            StructField("nconst", StringType(), False),            # PK: alphanumeric unique identifier of the name/person
            StructField("primaryName", StringType(), True),        # name by which the person is most often credited
            StructField("birthYear", StringType(), True),          # (YYYY or null)
            StructField("deathYear", StringType(), True),          # (YYYY, '\N', or null)   
            StructField("primaryProfession", StringType(), True),  # the top-3 professions of the person
            StructField("knownForTitles", StringType(), True)      # titles the person is known for
        ])

        nameBasics_path = hdfs_landing + "name.basics.tsv.gz"
        df = spark.read.format("csv") \
                        .option("header", True) \
                        .option("sep", "\t") \
                        .schema(schema) \
                        .load(f"{os.getenv('HDFS_FS_URL')}/{nameBasics_path}")
        
        # 2. Value Formatting
        df = value_formatting(df)

        # 3. Variable Formatting
        # - birthYear, deathYear --> convert to IntegerType
        df = df.withColumn("birthYear", F.col("birthYear").cast("integer")) \
               .withColumn("deathYear", F.col("deathYear").cast("integer"))
        
        # - primaryProfession, knownForTitles --> convert to arrays of strings
        df = df.withColumn("primaryProfession", F.split(F.col("primaryProfession"), ",")) \
               .withColumn("knownForTitles", F.split(F.col("knownForTitles"), ","))

        # 4. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)
        print(df.count())

        # 5. Save to parquet and analyze storage
        tmp_dir = "/tmp/IMDb"
        os.makedirs(tmp_dir, exist_ok=True)
        
        # Get original file size from HDFS
        original_size = hdfs_client.get_size(nameBasics_path)
        
        f_parquet = f"{tmp_dir}/name_basics.parquet"
        
        # Write with overwrite mode
        df.write.mode("overwrite").parquet(f_parquet)
        
        # Calculate Parquet directory size
        parquet_size = 0
        for dirpath, _, filenames in os.walk(f_parquet):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                parquet_size += os.path.getsize(fp)
        
        # Log size comparison
        log.info(f"Size reduction: {original_size} bytes → {parquet_size} bytes ({(1 - parquet_size/original_size)*100:.1f}% saved)")
        
        # Store in HDFS
        hdfs_client.copy_from_local(f_parquet, hdfs_formatted, overwrite=True)
        log.info(f"Transferred {f_parquet} to HDFS")

    except Exception as e:
        log.error(f"Pipeline failed: {str(e)}", exc_info=True)
        raise
    finally:
        if spark:
            spark.stop()
        log.info("Spark session closed.")