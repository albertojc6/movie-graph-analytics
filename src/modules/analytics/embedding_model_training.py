import torch
import os
import logging
from pykeen.pipeline import pipeline # type: ignore
from pykeen.triples import TriplesFactory # type: ignore

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)

def train_model():
    # [UPDATED PATHS]
    data_dir = "/opt/airflow/data/outputs"
    input_file = os.path.join(data_dir, "results.tsv")
    output_dir = os.path.join(data_dir, "pykeen_runs")

    if not os.path.exists(input_file):
        log.error(f"Input triples file not found: {input_file}")
        return

    log.info(f"Loading triples from {input_file}")
    tf = TriplesFactory.from_path(path=input_file)

    training, testing = tf.split(random_state=42)
    log.info(f"Entities: {len(tf.entity_to_id)}, Relations: {len(tf.relation_to_id)}")

    # Check for CUDA (GPU) availability, fallback to CPU
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    log.info(f"Training using device: {device}")

    results = pipeline(
        training=training,
        testing=testing,
        model='TransH',
        training_kwargs=dict(num_epochs=30, batch_size=2048),
        evaluation_kwargs={"batch_size": 4096},
        model_kwargs={"embedding_dim": 64},
        optimizer='Adam',
        optimizer_kwargs=dict(lr=1e-3),
        random_seed=42,
        device=device
    )

    # Persist
    results.save_to_directory(output_dir)
    log.info(f"Model saved to {output_dir}")

if __name__ == "__main__":
    train_model()