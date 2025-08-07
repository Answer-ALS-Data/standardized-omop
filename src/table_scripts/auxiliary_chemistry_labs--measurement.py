import pandas as pd
import logging
from datetime import datetime
from helpers import (
    relative_day_to_date,
    check_missing_concept_ids,
    get_visit_occurrence_id,
)

# Set up logging
logging.basicConfig(
    filename="logs/auxiliary_chemistry_labs--measurement.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def auxiliary_chemistry_labs_to_measurement(source_df, index_date_str):
    """
    Transform auxiliary chemistry labs data into OMOP measurement table format.

    Args:
        source_df (pd.DataFrame): Combined source data from Auxiliary_Chemistry_Labs.csv and Auxiliary_Chemistry.csv
        index_date_str (str): Index date in YYYY-MM-DD format

    Returns:
        pd.DataFrame: Transformed data in OMOP measurement table format
    """
    logging.info("Starting auxiliary chemistry labs to measurement transformation")
    logging.info(f"Source columns: {source_df.columns.tolist()}")

    # Convert index date string to datetime
    index_date = datetime.strptime(index_date_str, "%Y-%m-%d")

    # Initialize empty list to store transformed rows
    transformed_rows = []

    # Define the mapping of source variables to measurement concepts and meanings
    lab_mappings = {
        "acuarslt": {
            "concept_id": 4156643,
            "concept_name": "Blood urate measurement",
            "source_meaning": "Uric Acid",
            "unit_var": "acuaunit",  # Fixed unit variable name
            "unit_concept_id": 8840,
            "unit_concept_name": "milligram per deciliter",
            "norm_var": "uanorm",
        },
        "accrrslt": {
            "concept_id": 2212294,
            "concept_name": "Creatinine; blood",
            "source_meaning": "Creatinine",
            "unit_var": "accreuni",  # Fixed unit variable name
            "unit_concept_id": 8840,
            "unit_concept_name": "milligram per deciliter",
            "norm_var": "crenorm",
        },
        "acphrslt": {
            "concept_id": 4194292,
            "concept_name": "Blood inorganic phosphate measurement",
            "source_meaning": "Phosphorous",
            "unit_var": "acphouni",  # Fixed unit variable name
            "unit_concept_id": 8840,
            "unit_concept_name": "milligram per deciliter",
            "norm_var": "phonorm",
        },
        "acckrslt": {
            "concept_id": 3030170,
            "concept_name": "Creatine kinase measurement",
            "source_meaning": "Creatine Kinase (CK)",
            "unit_var": "acckunit",  # Fixed unit variable name
            "unit_concept_id": None,  # Will be determined based on unit
            "unit_concept_name": None,
            "norm_var": "cknorm",
        },
    }

    # Process each row in the source data
    for _, row in source_df.iterrows():
        person_id = row["Participant_ID"]

        # Calculate visit date using relative_day_to_date and format as YYYY-MM-DD
        visit_date = relative_day_to_date(row["labdt"], index_date)
        if visit_date is None:
            logging.warning(
                f"Skipping row for person_id {person_id} due to invalid labdt: {row['labdt']}"
            )
            continue
        visit_date_str = visit_date.strftime("%Y-%m-%d")

        # Process each lab measurement
        for source_var, mapping in lab_mappings.items():
            # Skip if result is missing
            if pd.isna(row[source_var]):
                logging.debug(
                    f"Skipping {source_var} for person_id {person_id} due to missing result"
                )
                continue

            # Get the corresponding unit variable
            unit_var = mapping["unit_var"]
            if unit_var not in row or pd.isna(row[unit_var]):
                logging.debug(
                    f"Skipping {source_var} for person_id {person_id} due to missing unit: {unit_var}"
                )
                continue

            # Get the corresponding norm variable
            norm_var = mapping["norm_var"]
            if norm_var not in row or pd.isna(row[norm_var]):
                logging.debug(
                    f"Skipping {source_var} for person_id {person_id} due to missing norm: {norm_var}"
                )
                continue

            # Build value_source_value with new format
            value_source_parts = []
            
            # Add result value
            # Convert to int if it's a numeric value to avoid float display
            result_value = row[source_var]
            if pd.notna(result_value) and isinstance(result_value, (int, float)):
                # Check if it's a whole number
                if result_value == int(result_value):
                    result_value = int(result_value)
            value_source_parts.append(f"auxiliary_chemistry_labs+{source_var} (lab result): {result_value}")
            
            # Add norm status
            norm_status = {
                1: "Normal",
                2: "Abnormal and Not Clinically Significant", 
                3: "Abnormal Clinically Significant",
            }
            norm_interpretation = norm_status.get(row[norm_var], 'Unknown')
            # Convert to int to ensure integer display
            norm_value = int(row[norm_var]) if pd.notna(row[norm_var]) else row[norm_var]
            value_source_parts.append(f"auxiliary_chemistry_labs+{norm_var} (norm status): {norm_value} ({norm_interpretation})")
            
            # Join the parts with ' | ' separator
            value_source_value = " | ".join(value_source_parts)

            # Determine value_as_concept_id based on norm status
            value_as_concept_id = 4069590 if row[norm_var] == 1 else 40641582

            # Special handling for creatine kinase units
            if source_var == "acckrslt":
                unit_concept_id = 8645 if "Units/L" in row[unit_var] else 8840
                unit_concept_name = (
                    "unit per liter"
                    if unit_concept_id == 8645
                    else "milligram per deciliter"
                )
            else:
                unit_concept_id = mapping["unit_concept_id"]
                unit_concept_name = mapping["unit_concept_name"]

            transformed_row = {
                "person_id": person_id,
                "measurement_concept_id": mapping["concept_id"],
                "measurement_concept_name": mapping["concept_name"],
                "measurement_source_value": f"auxiliary_chemistry_labs+{source_var} ({mapping['source_meaning']})",
                "measurement_date": visit_date_str,
                "measurement_type_concept_id": 32851,  # Healthcare professional filled survey
                "value_as_number": row[source_var],
                "value_as_concept_id": value_as_concept_id,
                "value_as_concept_name": "Normal" if row[norm_var] == 1 else "Abnormal",
                "value_source_value": value_source_value,
                "unit_concept_id": unit_concept_id,
                "unit_concept_name": unit_concept_name,
                "unit_source_value": f"auxiliary_chemistry_labs+{unit_var} (unit): {row[unit_var]}",
                "visit_occurrence_id": get_visit_occurrence_id(person_id, row["labdt"]),
            }
            transformed_rows.append(transformed_row)
            logging.debug(f"Added {source_var} measurement for person_id {person_id}")

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
        # Read both source data files - ensure labdt is read as integer
        labs_df = pd.read_csv(
            "source_tables/auxiliary_chemistry_labs.csv", dtype={"labdt": "Int64"}
        )
        logging.info(f"Read {len(labs_df)} rows from auxiliary_chemistry_labs.csv")

        chem_df = pd.read_csv(
            "source_tables/auxiliary_chemistry.csv", dtype={"labdt": "Int64"}
        )
        logging.info(f"Read {len(chem_df)} rows from auxiliary_chemistry.csv")

        # Combine the dataframes
        source_df = pd.concat([labs_df, chem_df], ignore_index=True)
        logging.info(f"Combined data contains {len(source_df)} rows")

        # Set index date to 2016-01-01
        index_date_str = "2016-01-01"

        # Transform data
        result_df = auxiliary_chemistry_labs_to_measurement(source_df, index_date_str)

        # Save to OMOP tables directory
        output_path = "processed_source/auxiliary_chemistry_labs--measurement.csv"
        result_df.to_csv(output_path, index=False)
        logging.info(f"Saved transformed data to {output_path}")

    except Exception as e:
        logging.error(f"Error in transformation: {str(e)}")
        raise


if __name__ == "__main__":
    main()
