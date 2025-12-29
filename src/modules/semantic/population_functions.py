from rdflib import Graph, URIRef, Literal, Namespace # type: ignore
from rdflib.namespace import RDF # type: ignore
import pandas as pd
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf, struct
import time
import requests
import logging
import os

# Configuration
GRAPHDB_ENDPOINT = "http://graphdb:7200/repositories/moviekg/statements"
BATCH_SIZE = 500
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# HDFS directories
# Assuming data is in /data/trusted/ in HDFS
HDFS_DIR = f"{os.getenv('HDFS_FS_URL')}/data/trusted/"

# IMDB
IMDB_NAME = HDFS_DIR + 'IMDb/name_basics.parquet'
IMDB_TITLE = HDFS_DIR + 'IMDb/title_basics.parquet'
IMDB_CREW = HDFS_DIR + 'IMDb/title_crew.parquet'

# TMDB
TMDB = HDFS_DIR + 'TMDb/crew_data.parquet'

# MovieTweetings
MT_MOVIES = HDFS_DIR + 'MovieTweetings/movies.parquet'
MT_RATINGS = HDFS_DIR + 'MovieTweetings/ratings.parquet'

# Namespaces
EX = Namespace("http://example.org/moviekg/")
XSD = Namespace("http://www.w3.org/2001/XMLSchema#")


def insert_to_graphdb(turtle_data):
    """Insert Turtle data into GraphDB with retry logic"""
    headers = {"Content-Type": "application/x-turtle"}
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(
                GRAPHDB_ENDPOINT,
                headers=headers,
                data=turtle_data,
                timeout=30
            )
            if response.status_code == 204:
                return True
            else:
                logging.info(f" Attempt {attempt + 1} failed: {response.status_code} - {response.text}")
        except Exception as e:
            logging.info(f" Attempt {attempt + 1} failed: {str(e)}")

        time.sleep(RETRY_DELAY * (attempt + 1))

    logging.info(f"Failed to insert batch after {MAX_RETRIES} attempts")
    return False


def execute_graph_population(source_path, graph_creation_function):
    """Initialize SparkSession locally and handle graph population"""
    spark = None
    try:
        # Create SparkSession inside the function
        # PYTHONPATH is set to /opt/airflow/dags so executors can find 'modules' package
        spark = SparkSession.builder \
            .config("spark.executorEnv.PYTHONPATH", "/opt/airflow/dags") \
            .appName(f"KG-Population-{graph_creation_function.__name__}") \
            .config("spark.executor.memory", "2g") \
            .getOrCreate()

        # --- UPDATED PATH ---
        # Add this specific file so executors have the 'graph_creation_function' definitions
        spark.sparkContext.addPyFile("/opt/airflow/dags/modules/semantic/population_functions.py")

        logging.info(f"📥 Processing {source_path}")
        df = spark.read.parquet(source_path)
        total_count = df.count()
        logging.info(f"Total records to process: {total_count}")

        @udf(StringType())
        def row_to_turtle(row):
            # Graph() is imported from rdflib inside UDF context via serialization
            g = Graph()
            g.bind("ex", EX)
            g.bind("xsd", XSD)
            try:
                graph_creation_function(g, row)
                return g.serialize(format="turtle")
            except Exception as e:
                logging.warning(f"Row processing error: {str(e)} on row {row}")
                return None

        def process_partition(partition):
            batch = []
            for row in partition:
                turtle_data = row['turtle']
                if turtle_data:
                    batch.append(turtle_data)
                    if len(batch) >= BATCH_SIZE:
                        combined_turtle = "\n".join(batch)
                        insert_to_graphdb(combined_turtle)
                        batch = []  # Reset batch

            # Process remaining records in the batch
            if batch:
                combined_turtle = "\n".join(batch)
                insert_to_graphdb(combined_turtle)

        turtle_df = df.withColumn("turtle", row_to_turtle(struct(*df.columns)))
        valid_turtle_df = turtle_df.filter("turtle IS NOT NULL").select("turtle")
        valid_turtle_df.foreachPartition(process_partition)

        logging.info(f"Finished processing {source_path}")
    finally:
        if spark:
            spark.stop()  # Ensure session is stopped

def create_graph_TMDB(g, row):
    if row['imdb_id'].startswith('nm'):
        person_uri = URIRef(f"{EX}Person/{row['imdb_id']}")
        g.add((person_uri,RDF.type,EX.Person))

        g.add((person_uri, EX.personId, Literal(row['imdb_id'], datatype=XSD.string)))
        g.add((person_uri, EX.name, Literal(row['name'], datatype=XSD.string)))
        g.add((person_uri, EX.gender, Literal(row['gender'], datatype=XSD.string)))
        g.add((person_uri, EX.popularity, Literal(row['popularity'], datatype=XSD.float)))

        if row['known_for'].startswith('tt'):
            movie_uri = URIRef(f"{EX}Movie/{row['known_for']}")
            g.add((movie_uri, RDF.type, EX.Movie))

            g.add((movie_uri, EX.isAdult, Literal(row['adult'], datatype=XSD.boolean)))

            known_for_uri = URIRef(f"{EX}Known_for/{row['imdb_id']}_{row['known_for']}")
            g.add((known_for_uri, RDF.type, EX.Known_for))

            g.add((person_uri, EX.isKnownfor, known_for_uri))
            g.add((known_for_uri, EX.known_for_movie, movie_uri))

            g.add((known_for_uri, EX.known_for_popularity, Literal(row['known_for_popularity'], datatype=XSD.float)))

    return g


def create_graph_Rating(g, row):
    user_uri = URIRef(f"{EX}User/{row['user_id']}")
    g.add((user_uri, RDF.type, EX.User))
    g.add((user_uri, EX.userId, Literal(row['user_id'], datatype=XSD.string)))
    if row['movie_id'].startswith('tt'):
        movie_uri = URIRef(f"{EX}Movie/{row['movie_id']}")
        g.add((movie_uri, RDF.type, EX.Movie))
        if pd.notna(row['rating']):
            movie_rating_uri = URIRef(f"{EX}Rating/{row['user_id']}_{row['movie_id']}")
            g.add((movie_rating_uri, RDF.type, EX.Rating))

            g.add((movie_rating_uri, EX.rating_date, Literal(row['rating_timestamp'], datatype=XSD.datetime)))
            g.add((movie_rating_uri, EX.rating_score, Literal(row['rating'], datatype=XSD.integer)))

            g.add((movie_rating_uri, EX.rating_movie, movie_uri))
            g.add((user_uri, EX.rates_movie, movie_rating_uri))

    return g


def create_graph_Movies(g, row):
    if row['movie_id'].startswith('tt'):
        movie_uri = URIRef(f"{EX}Movie/{row['movie_id']}")
        g.add((movie_uri, RDF.type, EX.Movie))

        g.add((movie_uri, EX.title, Literal(row['movie_title'], datatype=XSD.string)))
        g.add((movie_uri, EX.movieId, Literal(row['movie_id'], datatype=XSD.string)))

        genres = row['genres']
        # Check if 'genres' is a list and is not empty
        if isinstance(genres, (list, np.ndarray)):
            for genre in genres:
                # It's also good practice to check the genre itself isn't empty
                if genre and pd.notna(genre):
                    genre_uri = URIRef(f"{EX}Genre/{genre}")
                    g.add((genre_uri, RDF.type, EX.Genre))

                    g.add((genre_uri, EX.genre_name, Literal(genre, datatype=XSD.string)))
                    g.add((movie_uri, EX.has_genre, genre_uri))

    return g


def create_graph_name(g, row):
    if row['nconst'].startswith('nm'):
        person_uri = URIRef(f"{EX}Person/{row['nconst']}")
        g.add((person_uri, RDF.type, EX.Person))

        g.add((person_uri, EX.personId, Literal(row['nconst'], datatype=XSD.string)))
        g.add((person_uri, EX.name, Literal(row['primaryName'], datatype=XSD.string)))

        if pd.notna(row['birthYear']):
            g.add((person_uri, EX.birthYear, Literal(int(row['birthYear']), datatype=XSD.integer)))

        if pd.notna(row['deathYear']):
            g.add((person_uri, EX.deathYear, Literal(int(row['deathYear']), datatype=XSD.integer)))

        professions = row['primaryProfession']
        if isinstance(professions, (list, np.ndarray)):
            for i, profession in enumerate(professions):
                profession_uri = URIRef(f"{EX}Profession/{profession}")
                g.add((profession_uri, RDF.type, EX.Profession))

                g.add((profession_uri, EX.profession_name, Literal(professions[i], datatype=XSD.string)))

                if i == 0:
                    g.add((person_uri, EX.primary_profession, profession_uri))

                    known_for_titles = row['knownForTitles']

                    if isinstance(known_for_titles, (list, np.ndarray)):

                        for known_for_title in known_for_titles:
                            movie_uri = URIRef(f"{EX}Movie/{known_for_title}")
                            g.add((movie_uri, RDF.type, EX.Movie))
                            known_for_uri = URIRef(f"{EX}Known_for/{row['nconst']}_{known_for_title}")
                            g.add((known_for_uri, RDF.type, EX.Known_for))

                            g.add((person_uri, EX.isKnownfor, known_for_uri))
                            g.add((known_for_uri, EX.known_for_movie, movie_uri))

                else:
                    g.add((person_uri, EX.alternative_profession, profession_uri))

    return g


def create_graph_titles(g, row):
    if row['tconst'].startswith('tt'):
        movie_uri = URIRef(f"{EX}Movie/{row['tconst']}")
        g.add((movie_uri, RDF.type, EX.Movie))

        g.add((movie_uri, EX.title, Literal(row['primaryTitle'], datatype=XSD.string)))
        g.add((movie_uri, EX.movieId, Literal(row['tconst'], datatype=XSD.string)))
        g.add((movie_uri, EX.startYear, Literal(row['startYear'], datatype=XSD.integer)))
        g.add((movie_uri, EX.runtime, Literal(row['runtimeMinutes'], datatype=XSD.integer)))

        genres = row['genres']
        # Check if 'genres' is a list and is not empty
        if isinstance(genres, (list, np.ndarray)):
            for genre in genres:
                # It's also good practice to check the genre itself isn't empty
                if genre and pd.notna(genre):
                    genre_uri = URIRef(f"{EX}Genre/{genre}")
                    g.add((genre_uri, RDF.type, EX.Genre))

                    g.add((genre_uri, EX.genre_name, Literal(genre, datatype=XSD.string)))
                    g.add((movie_uri, EX.has_genre, genre_uri))

    return g


def create_graph_crew(g, row):
    if row['tconst'].startswith('tt'):
        movie_uri = URIRef(f"{EX}Movie/{row['tconst']}")
        g.add((movie_uri, RDF.type, EX.Movie))

        directors = row['directors']
        writers = row['writers']

        if isinstance(directors, (list, np.ndarray)):
            for director in directors:
                person_uri = URIRef(f"{EX}Person/{director}")
                g.add((person_uri, RDF.type, EX.Person))
                participates_uri = URIRef(f"{EX}Participates/{director}_{row['tconst']}")
                g.add((participates_uri, RDF.type, EX.Participates))
                profession_uri = URIRef(f"{EX}Profession/director")
                g.add((profession_uri, RDF.type, EX.Profession))

                g.add((participates_uri, EX.participates_role, profession_uri))
                g.add((participates_uri, EX.participates_person, person_uri))
                g.add((participates_uri, EX.participates_movie, movie_uri))

        if isinstance(writers, (list, np.ndarray)):
            for writer in writers:
                person_uri = URIRef(f"{EX}Person/{writer}")
                g.add((person_uri, RDF.type, EX.Person))
                participates_uri = URIRef(f"{EX}Participates/{writer}_{row['tconst']}")
                g.add((participates_uri, RDF.type, EX.Participates))
                profession_uri = URIRef(f"{EX}Profession/writer")
                g.add((profession_uri, RDF.type, EX.Profession))

                g.add((participates_uri, EX.participates_role, profession_uri))
                g.add((participates_uri, EX.participates_person, person_uri))
                g.add((participates_uri, EX.participates_movie, movie_uri))

    return g


def to_graph_IMDB_title():
    execute_graph_population(IMDB_TITLE, create_graph_titles)

def to_graph_IMDB_crew():
    execute_graph_population(IMDB_CREW, create_graph_crew)

def to_graph_IMDB_name():
    execute_graph_population(IMDB_NAME, create_graph_name)

def to_graph_MT_movies():
    execute_graph_population(MT_MOVIES, create_graph_Movies)

def to_graph_MT_rating():
    execute_graph_population(MT_RATINGS, create_graph_Rating)

def to_graph_TMDB():
    execute_graph_population(TMDB, create_graph_TMDB)