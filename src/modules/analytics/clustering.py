from sklearn.cluster import KMeans
import torch
import pandas as pd
import os
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def run_clustering():
    data_dir = "/opt/airflow/data/outputs"
    
    model_path = os.path.join(data_dir, 'trained_model.pkl')
    entity_path = os.path.join(data_dir, 'entity_to_id.tsv.zip')
    output_path = os.path.join(data_dir, 'movie_clusters.csv')

    if not os.path.exists(model_path):
        log.error(f"Model not found at {model_path}")
        return

    log.info("Loading model...")
    # Map to CPU since Airflow containers usually don't have GPU access
    model = torch.load(model_path, map_location=torch.device('cpu'))
    
    log.info("Loading entity mappings...")
    entity_to_id_df = pd.read_csv(entity_path, sep='\t', header=None, names=['id', 'entity'])
    
    # Filter IDs
    entity_to_id_df = entity_to_id_df[entity_to_id_df['id'].apply(lambda x: str(x).isdigit())]
    id_to_entity = {int(row['id']): row['entity'] for _, row in entity_to_id_df.iterrows()}

    # Filter movie entities
    movie_ids = [i for i, name in id_to_entity.items() if '/Movie/' in str(name)]
    movie_names = [id_to_entity[i] for i in movie_ids]

    if not movie_ids:
        log.warning("No movie entities found in embeddings.")
        return

    # Get embeddings
    embedding_module = model.entity_representations[0]
    all_embeddings = embedding_module(indices=None).detach().cpu().numpy()
    movie_embeddings = all_embeddings[movie_ids]

    # Clustering
    log.info("Running KMeans...")
    kmeans = KMeans(n_clusters=5, random_state=42)
    labels = kmeans.fit_predict(movie_embeddings)

    # Save results
    result_df = pd.DataFrame({
        'movie_entity': movie_names,
        'cluster': labels
    })
    result_df.to_csv(output_path, index=False)
    log.info(f"Clustering complete. Saved to {output_path}")

if __name__ == "__main__":
    run_clustering()