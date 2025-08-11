import pandas as pd
import logging
from datetime import datetime
from difflib import SequenceMatcher
from helpers import (
    relative_day_to_date,
    check_missing_concept_ids,
    get_visit_occurrence_id,
)
from pathlib import Path

# Set up logging
logging.basicConfig(
    filename="logs/vital_signs--measurement.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def is_similar_to_temporal(text):
    """
    Check if the given text is similar to 'Temporal' using fuzzy string matching.
    Returns True if the similarity ratio is above 0.8 (adjustable threshold).
    Case insensitive comparison.
    """
    if not isinstance(text, str):
        return False

    # Convert to lowercase for case-insensitive comparison
    text = text.lower()
    target = "temporal"

    # Direct match check
    if target in text:
        return True

    # Fuzzy match check
    ratio = SequenceMatcher(None, text, target).ratio()
    return ratio > 0.8


def vital_signs_to_measurement(source_df, index_date_str):
    """
    Transform vital signs data into OMOP measurement table format.

    Args:
        source_df (pd.DataFrame): Source data from Vital_Signs.csv
        index_date_str (str): Index date in YYYY-MM-DD format

    Returns:
        pd.DataFrame: Transformed data in OMOP measurement table format
    """
    logging.info("Starting vital signs to measurement transformation")
    logging.info(f"Source columns: {source_df.columns.tolist()}")

    # Convert index date string to datetime
    index_date = datetime.strptime(index_date_str, "%Y-%m-%d")

    # Initialize empty list to store transformed rows
    transformed_rows = []

    # Define the mapping of source variables to measurement concepts and meanings
    vital_sign_mappings = {
        "temp": {
            "temprt_mapping": {1: "Axillary", 2: "Oral", 3: "Rectal", 4: "Tympanic"},
            "concept_ids": {
                "Axillary": 4188706,
                "Oral": 3006322,
                "Rectal": 3022060,
                "Tympanic": 4215364,
                "Temporal": 46235152,
            },
            "unit_concept_ids": {1: 9289, 2: 586323},  # Fahrenheit  # Celsius
        },
        "bpsys": {
            "concept_id": 4152194,
            "unit_concept_id": 37546954,  # mmHg
            "value_as_concept_ids": {
                1: 4060833,  # Standing
                2: 4060834,  # Sitting
                3: 4060832,  # Supine
            },
        },
        "bpdias": {
            "concept_id": 4154790,
            "unit_concept_id": 37546954,  # mmHg
            "value_as_concept_ids": {
                1: 4060833,  # Standing
                2: 4060834,  # Sitting
                3: 4060832,  # Supine
            },
        },
        "hr": {"concept_id": 3027018, "unit_concept_id": 4118124},  # beats/min
        "rr": {"concept_id": 4313591, "unit_concept_id": 4117833},  # breaths/min
        "weight": {
            "concept_id": 3025315,
            "unit_concept_ids": {1: 8739, 2: 9529},  # pound (US)  # kilogram
        },
        "height": {
            "concept_id": 3036277,
            "unit_concept_ids": {1: 9330, 2: 8582},  # inch (US)  # centimeter
        },
        "bmi": {"concept_id": 3038553, "unit_concept_id": 8523},  # ratio
    }

    # Process each row in the source data
    for _, row in source_df.iterrows():
        person_id = row["Participant_ID"]

        # Calculate visit date using relative_day_to_date and format as YYYY-MM-DD
        # If vsdt is empty, use 1900-01-01 as default
        if pd.isna(row["vsdt"]):
            visit_date = datetime(1900, 1, 1)
            logging.warning(
                f"Using default date 1900-01-01 for person_id {person_id} due to empty vsdt"
            )
        else:
            visit_date = relative_day_to_date(row["vsdt"], index_date)
            if visit_date is None:
                logging.warning(
                    f"Skipping row for person_id {person_id} due to invalid vsdt: {row['vsdt']}"
                )
                continue
        visit_date_str = visit_date.strftime("%Y-%m-%d")

        # Process temperature measurements
        if not pd.isna(row["temp"]):
            logging.info(
                f"Processing temperature for person_id {person_id}: value={row['temp']}, temprt={row.get('temprt')}, temprtsp={row.get('temprtsp', 'N/A')}"
            )

            # First try to get temperature type from temprt integer mapping
            temp_type = None
            if not pd.isna(row.get("temprt")):
                try:
                    temp_type = vital_sign_mappings["temp"]["temprt_mapping"].get(
                        int(row["temprt"])
                    )
                    logging.info(f"Mapped temprt {row['temprt']} to {temp_type}")
                except (ValueError, TypeError):
                    logging.warning(f"Invalid temprt value: {row['temprt']}")

            # If no valid temprt, check temprtsp for Temporal
            if temp_type is None and not pd.isna(row.get("temprtsp")):
                if is_similar_to_temporal(row["temprtsp"]):
                    temp_type = "Temporal"
                    logging.info(
                        f"Found Temporal temperature type (fuzzy match) in temprtsp: {row['temprtsp']}"
                    )

            logging.info(f"Final temp_type: {temp_type}")

            if temp_type in vital_sign_mappings["temp"]["concept_ids"]:
                concept_id = vital_sign_mappings["temp"]["concept_ids"][temp_type]

                # If unit is missing, try to infer from value range
                if pd.isna(row["tempu"]):
                    temp_value = float(row["temp"])
                    # Check if value falls in normal Celsius range
                    if 35 <= temp_value <= 40:
                        unit_concept_id = vital_sign_mappings["temp"][
                            "unit_concept_ids"
                        ][
                            2
                        ]  # Celsius
                        unit_name = "degree Celsius"
                        unit_source = "C"
                        logging.info(
                            f"Inferred Celsius units for temperature {temp_value} (within 35-40°C range)"
                        )
                    # Check if value falls in normal Fahrenheit range
                    elif 95 <= temp_value <= 104:
                        unit_concept_id = vital_sign_mappings["temp"][
                            "unit_concept_ids"
                        ][
                            1
                        ]  # Fahrenheit
                        unit_name = "degree Fahrenheit"
                        unit_source = "F"
                        logging.info(
                            f"Inferred Fahrenheit units for temperature {temp_value} (within 95-104°F range)"
                        )
                    else:
                        logging.warning(
                            f"Could not infer temperature units for value {temp_value} - skipping record"
                        )
                        continue
                else:
                    unit_concept_id = vital_sign_mappings["temp"][
                        "unit_concept_ids"
                    ].get(row["tempu"])
                    if unit_concept_id is None:
                        logging.warning(
                            f"Invalid temperature unit value: {row['tempu']} - skipping record"
                        )
                        continue
                    unit_name = (
                        "degree Fahrenheit" if row["tempu"] == 1 else "degree Celsius"
                    )
                    unit_source = "F" if row["tempu"] == 1 else "C"

                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": concept_id,
                    "measurement_concept_name": (
                        f"{temp_type} temperature"
                        if temp_type != "Temporal"
                        else "Body temperature - Temporal artery"
                    ),
                    "measurement_source_value": f"vital_signs+temp ({temp_type} Temperature)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": row["temp"],
                    "unit_concept_id": unit_concept_id,
                    "unit_concept_name": unit_name,
                    "unit_source_value": unit_source,
                    "value_source_value": f"vital_signs+temp ({temp_type}): {row['temp']}",
                    "visit_occurrence_id": get_visit_occurrence_id(
                        person_id, row["vsdt"]
                    ),
                }
                transformed_rows.append(transformed_row)
            else:
                logging.warning(f"Unrecognized temperature type: {temp_type}")

        # Process blood pressure measurements
        if not pd.isna(row["bpsys"]):
            transformed_row = {
                "person_id": person_id,
                "measurement_concept_id": vital_sign_mappings["bpsys"]["concept_id"],
                "measurement_concept_name": "Systolic blood pressure",
                "measurement_source_value": "vital_signs+bpsys (Systolic Blood Pressure)",
                "measurement_date": visit_date_str,
                "measurement_type_concept_id": 32851,
                "value_as_number": row["bpsys"],
                "unit_concept_id": vital_sign_mappings["bpsys"]["unit_concept_id"],
                "unit_concept_name": "mmHg",
                "unit_source_value": "mmHG",
                "value_as_concept_id": vital_sign_mappings["bpsys"][
                    "value_as_concept_ids"
                ].get(row["bppos"]),
                "value_as_concept_name": (
                    "Standing blood pressure"
                    if row["bppos"] == 1
                    else (
                        "Sitting blood pressure"
                        if row["bppos"] == 2
                        else "Lying blood pressure"
                    )
                ),
                "value_source_value": f"vital_signs+bpsys: {row['bpsys']} | vital_signs+bppos ({('Standing' if row['bppos'] == 1 else 'Sitting' if row['bppos'] == 2 else 'Supine')}): {row['bppos']}",
                "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
            }
            transformed_rows.append(transformed_row)

        if not pd.isna(row["bpdias"]):
            transformed_row = {
                "person_id": person_id,
                "measurement_concept_id": vital_sign_mappings["bpdias"]["concept_id"],
                "measurement_concept_name": "Diastolic blood pressure",
                "measurement_source_value": "vital_signs+bpdias (Diastolic Blood Pressure)",
                "measurement_date": visit_date_str,
                "measurement_type_concept_id": 32851,
                "value_as_number": row["bpdias"],
                "unit_concept_id": vital_sign_mappings["bpdias"]["unit_concept_id"],
                "unit_concept_name": "mmHg",
                "unit_source_value": "mmHG",
                "value_as_concept_id": vital_sign_mappings["bpdias"][
                    "value_as_concept_ids"
                ].get(row["bppos"]),
                "value_as_concept_name": (
                    "Standing blood pressure"
                    if row["bppos"] == 1
                    else (
                        "Sitting blood pressure"
                        if row["bppos"] == 2
                        else "Lying blood pressure"
                    )
                ),
                "value_source_value": f"vital_signs+bpdias: {row['bpdias']} | vital_signs+bppos ({('Standing' if row['bppos'] == 1 else 'Sitting' if row['bppos'] == 2 else 'Supine')}): {row['bppos']}",
                "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
            }
            transformed_rows.append(transformed_row)

        # Process heart rate
        if not pd.isna(row["hr"]):
            transformed_row = {
                "person_id": person_id,
                "measurement_concept_id": vital_sign_mappings["hr"]["concept_id"],
                "measurement_concept_name": "Heart rate",
                "measurement_source_value": "vital_signs+hr (Heart rate)",
                "measurement_date": visit_date_str,
                "measurement_type_concept_id": 32851,
                "value_as_number": row["hr"],
                "unit_concept_id": vital_sign_mappings["hr"]["unit_concept_id"],
                "unit_concept_name": "beats/min",
                "unit_source_value": "Beats / min",
                "value_source_value": f"vital_signs+hr: {row['hr']}",
                "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
            }
            transformed_rows.append(transformed_row)

        # Process respiratory rate
        if not pd.isna(row["rr"]):
            transformed_row = {
                "person_id": person_id,
                "measurement_concept_id": vital_sign_mappings["rr"]["concept_id"],
                "measurement_concept_name": "Respiratory rate",
                "measurement_source_value": "vital_signs+rr (Respiratory Rate)",
                "measurement_date": visit_date_str,
                "measurement_type_concept_id": 32851,
                "value_as_number": row["rr"],
                "unit_concept_id": vital_sign_mappings["rr"]["unit_concept_id"],
                "unit_concept_name": "breaths/min",
                "unit_source_value": "Breaths / min",
                "value_source_value": f"vital_signs+rr: {row['rr']}",
                "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
            }
            transformed_rows.append(transformed_row)

        # Process weight
        if not pd.isna(row["weight"]):
            unit_concept_id = vital_sign_mappings["weight"]["unit_concept_ids"].get(
                row["weightu"]
            )
            if unit_concept_id is not None:
                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": vital_sign_mappings["weight"][
                        "concept_id"
                    ],
                    "measurement_concept_name": "Body weight",
                    "measurement_source_value": "vital_signs+weight (Weight)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": row["weight"],
                    "unit_concept_id": unit_concept_id,
                    "unit_concept_name": (
                        "pound (US)" if row["weightu"] == 1 else "kilogram"
                    ),
                    "unit_source_value": "lb" if row["weightu"] == 1 else "kg",
                    "value_source_value": f"vital_signs+weight: {row['weight']}",
                    "visit_occurrence_id": get_visit_occurrence_id(
                        person_id, row["vsdt"]
                    ),
                }
                transformed_rows.append(transformed_row)

        # Process height
        if not pd.isna(row["height"]):
            unit_concept_id = vital_sign_mappings["height"]["unit_concept_ids"].get(
                row["heightu"]
            )
            if unit_concept_id is not None:
                transformed_row = {
                    "person_id": person_id,
                    "measurement_concept_id": vital_sign_mappings["height"][
                        "concept_id"
                    ],
                    "measurement_concept_name": "Body height",
                    "measurement_source_value": "vital_signs+height (Height)",
                    "measurement_date": visit_date_str,
                    "measurement_type_concept_id": 32851,
                    "value_as_number": row["height"],
                    "unit_concept_id": unit_concept_id,
                    "unit_concept_name": (
                        "inch (US)" if row["heightu"] == 1 else "centimeter"
                    ),
                    "unit_source_value": "in" if row["heightu"] == 1 else "cm",
                    "value_source_value": f"vital_signs+height: {row['height']}",
                    "visit_occurrence_id": get_visit_occurrence_id(
                        person_id, row["vsdt"]
                    ),
                }
                transformed_rows.append(transformed_row)

        # Process BMI
        if not pd.isna(row["bmi"]):
            transformed_row = {
                "person_id": person_id,
                "measurement_concept_id": vital_sign_mappings["bmi"]["concept_id"],
                "measurement_concept_name": "Body mass index (BMI) [Ratio]",
                "measurement_source_value": "vital_signs+bmi (BMI)",
                "measurement_date": visit_date_str,
                "measurement_type_concept_id": 32851,
                "value_as_number": row["bmi"],
                "unit_concept_id": vital_sign_mappings["bmi"]["unit_concept_id"],
                "unit_concept_name": "ratio",
                "unit_source_value": "BMI",
                "value_source_value": f"vital_signs+bmi: {row['bmi']}",
                "visit_occurrence_id": get_visit_occurrence_id(person_id, row["vsdt"]),
            }
            transformed_rows.append(transformed_row)

    # Create DataFrame from transformed rows
    result_df = pd.DataFrame(transformed_rows)

    # Check for missing concept IDs
    check_missing_concept_ids(result_df, "measurement_concept_id")

    # Ensure all required columns are present
    required_columns = [
        "person_id",
        "measurement_concept_id",
        "measurement_concept_name",
        "measurement_source_value",
        "measurement_date",
        "measurement_type_concept_id",
        "value_as_number",
        "value_as_concept_id",
        "value_as_concept_name",
        "value_source_value",
        "unit_concept_id",
        "unit_concept_name",
        "unit_source_value",
        "visit_occurrence_id",
    ]

    for col in required_columns:
        if col not in result_df.columns:
            result_df[col] = None

    # Reorder columns
    result_df = result_df[required_columns]

    logging.info(
        f"Transformation complete. Created {len(result_df)} measurement records"
    )
    logging.info(
        f"Measurements by type: {result_df['measurement_source_value'].value_counts().to_dict()}"
    )
    return result_df


def main():
    try:
        # Read source data file
        source_df = pd.read_csv(
            "source_tables/vital_signs.csv", dtype={"vsdt": "Int64"}
        )
        logging.info(f"Read {len(source_df)} rows from vital_signs.csv")

        # Set index date to 2016-01-01
        index_date_str = "2016-01-01"

        # Transform data
        result_df = vital_signs_to_measurement(source_df, index_date_str)

        # Save to OMOP tables directory
        output_path = "processed_source/vital_signs--measurement.csv"
        result_df.to_csv(output_path, index=False)
        logging.info(f"Saved transformed data to {output_path}")

    except Exception as e:
        logging.error(f"Error in transformation: {str(e)}")
        raise


if __name__ == "__main__":
    main()
