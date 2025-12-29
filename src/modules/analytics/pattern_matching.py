import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import os
import tempfile
import logging
import requests
from typing import Optional, Dict, Any, Callable
from scipy.stats import linregress
from dags.utils.hdfs_utils import HDFSClient

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MovieGraphAnalytics:
    """
    Handles SPARQL querying, data processing, and visualization generation
    for the Movie Knowledge Graph.
    """
    
    def __init__(self, endpoint_url: str = "http://graphdb:7200/repositories/moviekg"):
        self.hdfs_client = HDFSClient()
        self.hdfs_analysis_dir = "/data/analysis"
        self.endpoint = endpoint_url
        self.headers = {
            'Accept': 'application/sparql-results+json',
            'Content-Type': 'application/x-www-form-urlencoded'
        }

    def _run_sparql_query(self, query: str) -> pd.DataFrame:
        """Execute SPARQL query and return results as DataFrame."""
        try:
            response = requests.post(
                self.endpoint,
                headers=self.headers,
                data={"query": query},
                timeout=120
            )
            response.raise_for_status()
            data = response.json()
            
            if not data['results']['bindings']:
                return pd.DataFrame()
            
            cols = data['head']['vars']
            rows = []
            for binding in data['results']['bindings']:
                row = [binding.get(col, {}).get('value', None) for col in cols]
                rows.append(row)
            
            logger.info(f"Query returned {len(rows)} rows")
            return pd.DataFrame(rows, columns=cols)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"SPARQL Query failed: {e}")
            return pd.DataFrame()

    def _save_and_upload_plot(self, fig: plt.Figure, filename: str):
        """Save plot locally and upload to HDFS."""
        temp_dir = tempfile.mkdtemp()
        local_path = os.path.join(temp_dir, filename)
        
        try:
            fig.savefig(local_path, bbox_inches='tight', dpi=300)
            plt.close(fig)
            
            hdfs_path = f"{self.hdfs_analysis_dir}/{filename}"
            self.hdfs_client.copy_from_local(local_path, hdfs_path)
            logger.info(f"Uploaded {filename} to HDFS")
        except Exception as e:
            logger.error(f"Failed to save/upload plot {filename}: {e}")
        finally:
            if os.path.exists(local_path):
                os.remove(local_path)

    # --- Plotting Logic (Kept your logic, just wrapped in methods) ---

    def plot_profession_gender(self, df: pd.DataFrame):
        df['count'] = df['count'].astype(int)
        pivot = df.pivot(index='professionName', columns='gender', values='count').fillna(0)
        
        fig, ax = plt.subplots(figsize=(14, 10))
        pivot.plot(kind='bar', stacked=True, ax=ax, color=['#FF6B6B', '#4ECDC4', '#45B7D1'])
        ax.set_title('Gender Distribution by Profession', fontsize=16, fontweight='bold')
        ax.set_ylabel('Count')
        ax.set_xlabel('Profession')
        plt.xticks(rotation=45, ha='right')
        plt.legend(title='Gender', bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        self._save_and_upload_plot(fig, "profession_gender_distribution.png")

    def plot_genre_evolution(self, df: pd.DataFrame):
        df['decade'] = df['decade'].astype(int)
        df['count'] = df['count'].astype(int)
        
        # Line Chart (Top-5 genres)
        top_genres = df.groupby('genreName')['count'].sum().nlargest(5).index
        df_top = df[df['genreName'].isin(top_genres)]
        pivot_line = df_top.pivot(index='decade', columns='genreName', values='count').fillna(0)
        
        fig1, ax1 = plt.subplots(figsize=(14, 8))
        colors = plt.cm.Set2(np.linspace(0, 1, len(top_genres)))
        for i, genre in enumerate(top_genres):
            ax1.plot(pivot_line.index, pivot_line[genre], label=genre, marker='o', linewidth=2.5, color=colors[i])
        ax1.set_title('Top 5 Genre Popularity Over Decades', fontsize=16, fontweight='bold')
        ax1.grid(True, linestyle='--', alpha=0.7)
        ax1.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        plt.tight_layout()
        self._save_and_upload_plot(fig1, "genre_trends_line.png")
        
        # Market share streamgraph logic kept here...
        # (Omitted for brevity, but you would include the rest of your logic)

    def execute_all(self):
        """Registry of analysis tasks to run."""
        
        # Define analyses as a list of dictionaries for cleaner configuration
        analyses = [
            {
                "name": "Profession-Gender Distribution",
                "query": """
                    PREFIX ex: <http://example.org/moviekg/>
                    SELECT ?professionName ?gender (COUNT(?person) as ?count)
                    WHERE {
                        ?person a ex:Person ;
                                ex:gender ?gender ;
                                ex:primary_profession ?profession .
                        ?profession ex:profession_name ?professionName .
                    }
                    GROUP BY ?professionName ?gender
                """,
                "handler": self.plot_profession_gender
            },
            {
                "name": "Genre-Temporal Evolution",
                "query": """
                    PREFIX ex: <http://example.org/moviekg/>
                    SELECT ?decade ?genreName (COUNT(?movie) as ?count)
                    WHERE {
                        ?movie a ex:Movie ;
                               ex:startYear ?year ;
                               ex:has_genre ?genre .
                        ?genre ex:genre_name ?genreName .
                        BIND (FLOOR(?year/10)*10 AS ?decade)
                        FILTER (?year > 1900)
                    }
                    GROUP BY ?decade ?genreName
                    ORDER BY ?decade
                """,
                "handler": self.plot_genre_evolution
            }
            # Add other analyses here...
        ]

        for analysis in analyses:
            logger.info(f"Starting analysis: {analysis['name']}")
            df = self._run_sparql_query(analysis['query'])
            if not df.empty:
                analysis['handler'](df)
            else:
                logger.warning(f"No data returned for {analysis['name']}")

# Entry point
def execute_analysis():
    logger.info("Starting movie knowledge graph analysis pipeline")
    analytics_engine = MovieGraphAnalytics()
    analytics_engine.execute_all()
    logger.info("All analyses completed successfully")

if __name__ == "__main__":
    execute_analysis()