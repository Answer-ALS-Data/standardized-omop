import logging
from pathlib import Path

logging.basicConfig(
    filename="logs/medical_history--drug_exposure.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

source_file = "source_tables/medical_history.csv"

output_file = "processed_source/medical_history--drug_exposure.csv"
