import pandas as pd
import logging
from helpers import (
    relative_day_to_date,
    check_missing_concept_ids,
    get_visit_occurrence_id,
)
import os
from pathlib import Path
from datetime import datetime


# Make os relative path from directory names
logs_dir = os.path.join("logs", "aalshxfx--condition_occurrence.log")
processed_source_dir = os.path.join("processed_source", "aalshxfx.csv")
source_tables_dir = os.path.join("source_tables", "aalshxfx.csv")

# Set up logging
logging.basicConfig(
    filename=logs_dir,
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def main():
    try:
        # Read source data
        source_data = pd.read_csv(source_tables_dir)

        # Initialize output dataframe
        output_columns = [
            "person_id",
            "condition_concept_id",
            "condition_concept_name",
            "condition_source_value",
            "condition_start_date",
            "condition_type_concept_id",
            "visit_occurrence_id",
        ]
        output_data = pd.DataFrame(columns=output_columns)

        # Set index date
        index_date = datetime.strptime("2016-01-01", "%Y-%m-%d")

        # Process each row
        for _, row in source_data.iterrows():
            # Get person_id
            person_id = row["Participant_ID"]

            # Process diagnosis date
            if pd.notna(row.get("diagdt")):
                condition_start_date = relative_day_to_date(row["diagdt"], index_date)
                output_data = pd.concat(
                    [
                        output_data,
                        pd.DataFrame(
                            [
                                {
                                    "person_id": person_id,
                                    "condition_concept_id": 373182,
                                    "condition_concept_name": "Amyotrophic lateral sclerosis",
                                    "condition_source_value": "Date of ALS diagnosis",
                                    "condition_start_date": condition_start_date,
                                    "condition_type_concept_id": 32851,
                                    "visit_occurrence_id": f"{person_id}_{row['Visit_Date']}",
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )

            # Process symptom onset date
            if pd.notna(row.get("onsetdt")):
                condition_start_date = relative_day_to_date(row["onsetdt"], index_date)
                output_data = pd.concat(
                    [
                        output_data,
                        pd.DataFrame(
                            [
                                {
                                    "person_id": person_id,
                                    "condition_concept_id": "2000000397",
                                    "condition_concept_name": "ALS symptom onset",
                                    "condition_source_value": "Date of ALS symptom onset",
                                    "condition_start_date": condition_start_date,
                                    "condition_type_concept_id": 32851,
                                    "visit_occurrence_id": f"{person_id}_{row['Visit_Date']}",
                                }
                            ]
                        ),
                    ],
                    ignore_index=True,
                )

        # Check for missing concept IDs
        check_missing_concept_ids(output_data, ["condition_concept_id"])

        # Save output
        output_data.to_csv(
            processed_source_dir, index=False
        )

        logging.info("Successfully completed ETL process")

    except Exception as e:
        logging.error(f"Error in ETL process: {str(e)}")
        raise


if __name__ == "__main__":
    main()
