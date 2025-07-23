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

# Load concept mapping
concept_csv_path = os.path.join("source_tables", "omop_tables", "concept.csv")
concept_df = pd.read_csv(concept_csv_path, dtype={"concept_id": str})
concept_id_to_name = dict(zip(concept_df["concept_id"], concept_df["concept_name"]))

# Load subject group mapping
subjects_csv_path = os.path.join("source_tables", "subjects.csv")
subjects_df = pd.read_csv(subjects_csv_path, dtype={"Participant_ID": str, "subject_group_id": str})
subject_group_map = dict(zip(subjects_df["Participant_ID"], subjects_df["subject_group_id"]))

# Add subject group interpretation mapping
subject_group_interpretation = {
    "1": "ALS",
    "17": "Non-ALS MND",
}


# Make os relative path from directory names
logs_dir = os.path.join("logs", "aalshxfx--condition_occurrence.log")
processed_source_dir = os.path.join("processed_source", "aalshxfx--condition_occurrence.csv")
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
            subject_group_id = subject_group_map.get(person_id)
            if subject_group_id is None:
                continue  # skip if no group info

            # ALS group
            if subject_group_id == "1":
                # Process diagnosis date
                if pd.notna(row.get("diagdt")):
                    condition_start_date = relative_day_to_date(row["diagdt"], index_date)
                    diagnosis_concept_id = "373182"
                    diagnosis_concept_name = "Amyotrophic lateral sclerosis"
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    condition_source_value = f"{group_part} | aalshxfx+from_ecrf: Date of ALS diagnosis"
                    output_data = pd.concat(
                        [
                            output_data,
                            pd.DataFrame(
                                [
                                    {
                                        "person_id": person_id,
                                        "condition_concept_id": diagnosis_concept_id,
                                        "condition_concept_name": diagnosis_concept_name,
                                        "condition_source_value": condition_source_value,
                                        "condition_start_date": condition_start_date,
                                        "condition_type_concept_id": 32851,
                                        "visit_occurrence_id": f"{person_id}_{row['Visit_Date']}",
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )
                # Process ALS symptom onset date
                if pd.notna(row.get("onsetdt")):
                    condition_start_date = relative_day_to_date(row["onsetdt"], index_date)
                    onset_concept_id = "2000000397"
                    onset_concept_name = concept_id_to_name[onset_concept_id]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    condition_source_value = f"{group_part} | aalshxfx+from_ecrf: Date of ALS symptom onset"
                    output_data = pd.concat(
                        [
                            output_data,
                            pd.DataFrame(
                                [
                                    {
                                        "person_id": person_id,
                                        "condition_concept_id": onset_concept_id,
                                        "condition_concept_name": onset_concept_name,
                                        "condition_source_value": condition_source_value,
                                        "condition_start_date": condition_start_date,
                                        "condition_type_concept_id": 32851,
                                        "visit_occurrence_id": f"{person_id}_{row['Visit_Date']}",
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )
            # Non-ALS MND group
            elif subject_group_id == "17":
                # Only process MND symptom onset (no diagnosis)
                if pd.notna(row.get("onsetdt")):
                    condition_start_date = relative_day_to_date(row["onsetdt"], index_date)
                    onset_concept_id = "2000002019"
                    onset_concept_name = concept_id_to_name[onset_concept_id]
                    group_interp = subject_group_interpretation.get(subject_group_id, "")
                    if group_interp:
                        group_part = f"subjects+subject_group_id: {subject_group_id} ({group_interp})"
                    else:
                        group_part = f"subjects+subject_group_id: {subject_group_id}"
                    condition_source_value = f"{group_part} | aalshxfx+from_ecrf: Date of MND symptom onset"
                    output_data = pd.concat(
                        [
                            output_data,
                            pd.DataFrame(
                                [
                                    {
                                        "person_id": person_id,
                                        "condition_concept_id": onset_concept_id,
                                        "condition_concept_name": onset_concept_name,
                                        "condition_source_value": condition_source_value,
                                        "condition_start_date": condition_start_date,
                                        "condition_type_concept_id": 32851,
                                        "visit_occurrence_id": f"{person_id}_{row['Visit_Date']}",
                                    }
                                ]
                            ),
                        ],
                        ignore_index=True,
                    )
            # All other groups: skip

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
