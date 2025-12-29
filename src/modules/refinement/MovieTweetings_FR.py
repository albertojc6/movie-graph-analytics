from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
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
hdfs_landing = "/data/landing/MovieTweetings/"
hdfs_formatted = "/data/formatted/MovieTweetings"

def format_MovieTweetings(hdfs_client: HDFSClient):
    """
    Formats MovieTweetings data according to a common relational data model, using HDFS

    Args:
        hdfs_client: object for interacting with HDFS easily.
    """

    # Clean up temporary files from previous runs
    tmp_dir = "/tmp/MovieTweetings"
    if os.path.exists(tmp_dir):
        shutil.rmtree(tmp_dir)
    os.makedirs(tmp_dir, exist_ok=True)

    # Format each dataset from MovieTweetings with its corresponding processing
    format_movies(hdfs_client)
    format_users(hdfs_client)
    format_ratings(hdfs_client)


def value_formatting(df):
    """
    Defines correct format for all string values, so as to gain consistency.
    """
    # Collapse multiple spaces and trim all string columns -> consistency!
    string_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    for col in string_cols:
        df = df.withColumn(col, F.trim(F.regexp_replace(F.col(col), r"\s+", " ")))

    # - Homogenize NA indicators
    na_values = ['', 'NA', 'N/A', 'NaN', 'NULL']
    for col in string_cols:
        df = df.withColumn(col, F.when(F.col(col).isin(na_values), None).otherwise(F.col(col)))
    
    return df

def format_movies(hdfs_client: HDFSClient):
    """
    Formats movies dataset from MovieTweetings

    Args:
        hdfs_client: object for interacting with HDFS easily.
    """

    spark = None
    log.info("Formatting the movies dataset from MovieTweetings ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .appName("MoviesFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema =  StructType([
            StructField("movie_id", StringType(), False), # PK: unique identifier of the movie
            StructField("movie_title", StringType(), True), # title of the movie including year
            StructField("genres", StringType(), True) # pipe-separated list of genres
        ])

        movies_path = hdfs_landing + "movies.dat"
        df = spark.read.schema(schema).option("sep", "::").csv(f"{os.getenv('HDFS_FS_URL')}/{movies_path}")

        # 2. Variable Formatting
        # - movie_title --> movie_title (movie_year): split and Data Types Conversion!
        df = df.withColumn("movie_year", F.regexp_extract(F.col("movie_title"), r"\((\d{4})\)$", 1).cast("int")) \
                .withColumn("movie_title", F.regexp_replace(F.col("movie_title"), r"\s*\(\d{4}\)$", "").cast("string"))
        
        # - movie_title --> homogenize strings
        unidecode_udf = F.udf(lambda s: unidecode(s) if s else None, StringType())
        df = df.withColumn("movie_title", unidecode_udf(F.col("movie_title"))) # Remove diacritics/accents (e.g., À -> A, é -> e)

        # - genres --> genre1|genre2|genre3: split into an array column [genre1, genre2, genre3]
        df = df.withColumn("genres", F.split(F.col("genres"), "\\|"))

        # - movie_id --> add "tt" for homogenization with IMDB movie identfiers
        df = df.withColumn("movie_id", F.concat(F.lit("tt"), df["movie_id"]))

        # 3. Value Formatting
        df = value_formatting(df)

        # 4. Reorganize columns
        df = df.select("movie_id", "movie_title", "movie_year", "genres")

        # 5. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)
        print(df.count())

        # 6. Save to parquet and analyze storage
        tmp_dir = "/tmp/MovieTweetings"
        os.makedirs(tmp_dir, exist_ok=True)
        
        # Get original file size from HDFS
        original_size = hdfs_client.get_size(movies_path)
        
        f_parquet = f"{tmp_dir}/movies.parquet"
        
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

def format_users(hdfs_client: HDFSClient):
    """
    Formats users dataset from MovieTweetings

    Args:
        hdfs_client: object for interacting with HDFS easily.
    """

    spark = None
    log.info("Formatting the users dataset from MovieTweetings ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .appName("UsersFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema = StructType([
            StructField("user_id", StringType(), False), # PK: unique identifier of the user
            StructField("twitter_id", StringType(), True) # corresponding Twitter ID if available
        ])

        users_path = hdfs_landing + "users.dat"
        df = spark.read.schema(schema).option("sep", "::").csv(f"{os.getenv('HDFS_FS_URL')}/{users_path}")

        # 2. Value Formatting
        df = value_formatting(df)

        # 3. Visualize formatting results
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)
        print(df.count())

        # 4. Save to parquet and analyze storage
        tmp_dir = "/tmp/MovieTweetings"
        os.makedirs(tmp_dir, exist_ok=True)
        
        # Get original file size from HDFS
        original_size = hdfs_client.get_size(users_path)
        
        f_parquet = f"{tmp_dir}/users.parquet"
        
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

def format_ratings(hdfs_client: HDFSClient):
    """
    Formats ratings dataset from MovieTweetings

    Args:
        hdfs_client: object for interacting with HDFS easily.
    """

    spark = None
    log.info("Formatting the ratings dataset from MovieTweetings ...")
    try:
        # Connection to Spark
        spark = SparkSession.builder \
            .appName("RatingsFormatter") \
            .getOrCreate()

        # 1. Read data from RDD in HDFS and create schema
        schema = StructType([
            StructField("user_id", StringType(), False), # PK: unique identifier of the user
            StructField("movie_id", StringType(), False), # PK: unique identifier of the movie
            StructField("rating", StringType(), True), # rating given by the user (1-10)
            StructField("rating_timestamp", StringType(), True) # UNIX timestamp of when the rating was given
        ])

        ratings_path = hdfs_landing + "ratings.dat"
        df = spark.read.schema(schema).option("sep", "::").csv(f"{os.getenv('HDFS_FS_URL')}/{ratings_path}")

        # 2. Variable Formatting
        # - rating --> convert from String to Integer
        df = df.withColumn("rating", F.col("rating").cast(IntegerType()))

        # - rating_timestamp --> convert UNIX timestamp to proper timestamp type
        df = df.withColumn("rating_timestamp", F.from_unixtime(F.col("rating_timestamp")).cast(TimestampType()))

        # - movie_id --> add "tt" for homogenization with IMDB movie identifiers
        df = df.withColumn("movie_id", F.concat(F.lit("tt"), df["movie_id"]))

        # 3. Value Formatting
        df = value_formatting(df)

        # 4. Visualize formatting results   
        log.info("Final Schema:")
        df.printSchema()
        log.info("Sample Data:")
        df.show(5, truncate=False)
        print(df.count())

        # 5. Save to parquet and analyze storage
        tmp_dir = "/tmp/MovieTweetings"
        os.makedirs(tmp_dir, exist_ok=True)
        
        # Get original file size from HDFS
        original_size = hdfs_client.get_size(ratings_path)
        
        f_parquet = f"{tmp_dir}/ratings.parquet"
        
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