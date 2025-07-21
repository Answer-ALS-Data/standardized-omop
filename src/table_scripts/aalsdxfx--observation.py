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
    filename="logs/aalsdxfx--observation.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


def aalsdx1_to_observation_value_as_concept_id(value):
    """Convert alsdx1 value to concept ID"""
    mapping = {
        1: 45877994,  # Yes
        2: 45878245,  # No
        90: 45881531,  # Not assessed
    }
    return mapping.get(value, 0)


def aalsdx1_to_observation_value_source_value(value):
    """Convert alsdx1 value to source value"""
    mapping = {
        1: "Yes",
        2: "No",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


def aalsdx2_to_observation_value_as_concept_id(value):
    """Convert alsdx2 value to concept ID"""
    mapping = {
        1: 45877994,  # Yes
        2: 45878245,  # No
        90: 45881531,  # Not assessed
    }
    return mapping.get(value, 0)


def aalsdx2_to_observation_value_source_value(value):
    """Convert alsdx2 value to source value"""
    mapping = {
        1: "Yes",
        2: "No",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


def aalsdx3_to_observation_value_as_concept_id(value):
    """Convert alsdx3 value to concept ID"""
    mapping = {
        1: 45877994,  # Yes
        2: 45878245,  # No
        90: 45881531,  # Not assessed
    }
    return mapping.get(value, 0)


def aalsdx3_to_observation_value_source_value(value):
    """Convert alsdx3 value to source value"""
    mapping = {
        1: "Yes",
        2: "No",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


def elescrlr_to_observation_value_as_concept_id(value):
    """Convert elescrlr value to concept ID"""
    mapping = {
        1: 2000000062,  # Suspected
        2: 2000000058,  # Possible
        3: 2000000060,  # Probable Laboratory Supported
        4: 2000000059,  # Probable
        5: 2000000057,  # Definite
    }
    return mapping.get(value, 0)


def elescrlr_to_observation_value_source_value(value):
    """Convert elescrlr value to source value"""
    mapping = {
        1: "Suspected",
        2: "Possible",
        3: "Probable Laboratory Supported",
        4: "Probable",
        5: "Definite",
    }
    return mapping.get(value, "Unknown")


def clinical_indicator_to_observation_value_as_concept_id(value):
    """Convert clinical indicator value to concept ID"""
    mapping = {
        1: 45877994,  # Yes
        2: 45878245,  # No
        90: 45881531,  # Not assessed
    }
    return mapping.get(value, 0)


def clinical_indicator_to_observation_value_source_value(value):
    """Convert clinical indicator value to source value"""
    mapping = {
        1: "Yes",
        2: "No",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


def emg_indicator_to_observation_value_as_concept_id(value):
    """Convert EMG indicator value to concept ID"""
    mapping = {
        1: 45877994,  # Yes (Denervation)
        2: 45878245,  # No (No Denervation)
        90: 45881531,  # Not assessed (Not Done)
    }
    return mapping.get(value, 0)


def emg_indicator_to_observation_value_source_value(value):
    """Convert EMG indicator value to source value"""
    mapping = {
        1: "Denervation",
        2: "No Denervation",
        90: "Not Done",
    }
    return mapping.get(value, "Unknown")


def aalsdx1_to_observation_value_as_concept_name(value):
    """Convert alsdx1 value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def aalsdx2_to_observation_value_as_concept_name(value):
    """Convert alsdx2 value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def aalsdx3_to_observation_value_as_concept_name(value):
    """Convert alsdx3 value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def elescrlr_to_observation_value_as_concept_name(value):
    """Convert elescrlr value to concept name"""
    mapping = {
        1: "Suspected",  # 2000000062
        2: "Possible",  # 2000000058
        3: "Probable Laboratory Supported",  # 2000000060
        4: "Probable",  # 2000000059
        5: "Definite",  # 2000000057
    }
    return mapping.get(value, "Unknown")


def clinical_indicator_to_observation_value_as_concept_name(value):
    """Convert clinical indicator value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def emg_indicator_to_observation_value_as_concept_name(value):
    """Convert EMG indicator value to concept name"""
    mapping = {
        1: "yes",  # 45877994
        2: "no",  # 45878245
        90: "not assessed",  # 45881531
    }
    return mapping.get(value, "Unknown")


def process_aalsdxfx_to_observation(source_file, index_date):
    """
    Process AALSDXFX data into OMOP observation format

    Args:
        source_file (str): Path to the source AALSDXFX.csv file
        index_date (datetime): Reference date for relative day calculations
    """
    try:
        # Read source data
        df = pd.read_csv(source_file)
        logging.info(f"Successfully read {len(df)} rows from {source_file}")

        # Initialize empty list to store observations
        observations = []

        # Define observation items and their mappings
        observation_items = [
            {
                "source_column": "alsdx1",
                "concept_id": "als_diagnosis_presence_lmn",
                "concept_name": "Presence of evidence of lower motor neuron (LMN) degeneration by clinical, electrophysiological or neuropathologic examination",
                "source_value": "Topographical location and pattern of progression of UMN and LMN signs, including signs of spread within a region or to other regions, consistent with ALS?",
                "value_converter": aalsdx1_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx1_to_observation_value_source_value,
                "concept_name_converter": aalsdx1_to_observation_value_as_concept_name,
            },
            {
                "source_column": "alsdx1",
                "concept_id": "als_diagnosis_presence_umn",
                "concept_name": "Presence of evidence of upper motor neuron (UMN) degeneration by clinical examination",
                "source_value": "Topographical location and pattern of progression of UMN and LMN signs, including signs of spread within a region or to other regions, consistent with ALS?",
                "value_converter": aalsdx1_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx1_to_observation_value_source_value,
                "concept_name_converter": aalsdx1_to_observation_value_as_concept_name,
            },
            {
                "source_column": "alsdx1",
                "concept_id": 2000000020,
                "concept_name": "Presence of progressive spread of symptoms or signs within a region or to other regions as determined by history or examination",
                "source_value": "Topographical location and pattern of progression of UMN and LMN signs, including signs of spread within a region or to other regions, consistent with ALS?",
                "value_converter": aalsdx1_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx1_to_observation_value_source_value,
                "concept_name_converter": aalsdx1_to_observation_value_as_concept_name,
            },
            {
                "source_column": "alsdx2",
                "concept_id": 2000000021,
                "concept_name": "Absence of electrophysiological or pathological evidence of other disease processes that might explain the signs of LMN and/or UMN degeneration",
                "source_value": "Exclusion by electrophysiological testing of all other processes including conduction block that might explain the underlying signs and symptoms?",
                "value_converter": aalsdx2_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx2_to_observation_value_source_value,
                "concept_name_converter": aalsdx2_to_observation_value_as_concept_name,
            },
            {
                "source_column": "alsdx3",
                "concept_id": 2000000022,
                "concept_name": "Absence of neuroimaging evidence of other disease processes that might explain the observed clinical and electrophysiological signs",
                "source_value": "Exclusion by neuroimaging of other disease processes such as myelopathy or radiculopathy that might explain observed clinical and electrophysiological signs?",
                "value_converter": aalsdx3_to_observation_value_as_concept_id,
                "source_value_converter": aalsdx3_to_observation_value_source_value,
                "concept_name_converter": aalsdx3_to_observation_value_as_concept_name,
            },
            {
                "source_column": "elescrlr",
                "concept_id": 2000000061,
                "concept_name": "El Escorial criteria - revised",
                "source_value": "Revised El Escorial Criteria for ALS",
                "value_converter": elescrlr_to_observation_value_as_concept_id,
                "source_value_converter": elescrlr_to_observation_value_source_value,
                "concept_name_converter": elescrlr_to_observation_value_as_concept_name,
            },
            {
                "source_column": "blbcumn",
                "concept_id": 2000000035,
                "concept_name": "Bulbar upper motor neuron clinical indicator",
                "source_value": "Bulbar upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "luecumn",
                "concept_id": "lueumnclinind",
                "concept_name": "Left upper extremity upper motor neuron clinical indicator",
                "source_value": "Left upper extremity upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "ruecumn",
                "concept_id": "rueumnclinind",
                "concept_name": "Right upper extremity upper motor neuron clinical indicator",
                "source_value": "Right upper extremity upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "trnkcumn",
                "concept_id": "trunkumnclinind",
                "concept_name": "Trunk upper motor neuron clinical indicator",
                "source_value": "Trunk upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "llecumn",
                "concept_id": "lleumnclinind",
                "concept_name": "Left lower extremity upper motor neuron clinical indicator",
                "source_value": "Left lower extremity upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rlecumn",
                "concept_id": "rleumnclinind",
                "concept_name": "Right lower extremity upper motor neuron clinical indicator",
                "source_value": "Right lower extremity upper motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "blbclmn",
                "concept_id": 2000000029,
                "concept_name": "Bulbar lower motor neuron clinical indicator",
                "source_value": "Bulbar lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "lueclmn",
                "concept_id": "luelmnclinind",
                "concept_name": "Left upper extremity lower motor neuron clinical indicator",
                "source_value": "Left upper extremity lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rueclmn",
                "concept_id": "ruelmnclinind",
                "concept_name": "Right upper extremity lower motor neuron clinical indicator",
                "source_value": "Right upper extremity lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "trnkclmn",
                "concept_id": "trunklmnclinind",
                "concept_name": "Trunk lower motor neuron clinical indicator",
                "source_value": "Trunk lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "lleclmn",
                "concept_id": "llelmnclinind",
                "concept_name": "Left lower extremity lower motor neuron clinical indicator",
                "source_value": "Left lower extremity lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rleclmn",
                "concept_id": "rlelmnclinind",
                "concept_name": "Right lower extremity lower motor neuron clinical indicator",
                "source_value": "Right lower extremity lower motor neuron clinical indicator",
                "value_converter": clinical_indicator_to_observation_value_as_concept_id,
                "source_value_converter": clinical_indicator_to_observation_value_source_value,
                "concept_name_converter": clinical_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "blbelmn",
                "concept_id": 2000000030,
                "concept_name": "Bulbar lower motor neuron electromyogram indicator",
                "source_value": "Bulbar lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "lueelmn",
                "concept_id": "luelmnemgind",
                "concept_name": "Left upper extremity lower motor neuron electromyogram indicator",
                "source_value": "Left upper extremity lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rueelmn",
                "concept_id": "ruelmnemgind",
                "concept_name": "Right upper extremity lower motor neuron electromyogram indicator",
                "source_value": "Right upper extremity lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "trnkelmn",
                "concept_id": "trunklmnemgind",
                "concept_name": "Trunk lower motor neuron electromyogram indicator",
                "source_value": "Trunk lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "lleelmn",
                "concept_id": "llelmnemgind",
                "concept_name": "Left lower extremity lower motor neuron electromyogram indicator",
                "source_value": "Left lower extremity lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
            {
                "source_column": "rleelmn",
                "concept_id": "rlelmnemgind",
                "concept_name": "Right lower extremity lower motor neuron electromyogram indicator",
                "source_value": "Right lower extremity lower motor neuron electromyogram indicator",
                "value_converter": emg_indicator_to_observation_value_as_concept_id,
                "source_value_converter": emg_indicator_to_observation_value_source_value,
                "concept_name_converter": emg_indicator_to_observation_value_as_concept_name,
            },
        ]

        # Process each row in the source data
        for _, row in df.iterrows():
            person_id = row["Participant_ID"]

            # Handle NaN values in alsdxdt
            if pd.isna(row["alsdxdt"]):
                visit_occurrence_id = None
            else:
                visit_occurrence_id = get_visit_occurrence_id(
                    person_id, int(row["Visit_Date"])
                )

            # Process each observation item
            for item in observation_items:
                if item["source_column"] in row and pd.notna(
                    row[item["source_column"]]
                ):
                    value = int(row[item["source_column"]])
                    value_as_concept_id = item["value_converter"](value)
                    value_source_value = item["source_value_converter"](value)
                    value_as_concept_name = item["concept_name_converter"](value)

                    # Calculate observation date from alsdxdt
                    observation_date = None
                    if pd.notna(row["alsdxdt"]):
                        relative_day = int(row["alsdxdt"])
                        observation_date = relative_day_to_date(
                            relative_day, index_date
                        )
                        logging.info(
                            f"Converting alsdxdt: {relative_day} to date: {observation_date}"
                        )

                    observation = {
                        "person_id": person_id,
                        "observation_concept_id": item["concept_id"],
                        "observation_concept_name": item["concept_name"],
                        "observation_source_value": item["source_value"],
                        "observation_date": observation_date,
                        "observation_type_concept_id": 32851,  # Healthcare professional filled survey
                        "value_as_number": "",  # Intentionally empty
                        "value_as_string": "",  # Intentionally empty
                        "value_as_concept_id": value_as_concept_id,
                        "value_as_concept_name": value_as_concept_name,
                        "value_source_value": value_source_value,
                        "qualifier_concept_id": "",  # Intentionally empty
                        "qualifier_concept_name": "",  # Intentionally empty
                        "qualifier_source_value": "",  # Intentionally empty
                        "unit_concept_id": "",  # Intentionally empty
                        "unit_concept_name": "",  # Intentionally empty
                        "unit_source_value": "",  # Intentionally empty
                        "visit_occurrence_id": visit_occurrence_id,
                        "observation_event_id": "",  # Intentionally empty
                        "obs_event_field_concept_id": "",  # Intentionally empty
                    }
                    observations.append(observation)

        # Create DataFrame from observations
        result_df = pd.DataFrame(observations)

        # Check for missing concept IDs only on relevant columns
        concept_columns = [
            "observation_concept_id",
            "observation_concept_name",
            "value_as_concept_id",
            "value_as_concept_name",
        ]
        result_df_concepts = result_df[concept_columns].copy()
        result_df_concepts = check_missing_concept_ids(result_df_concepts)

        # Update only the concept columns in the original DataFrame
        for col in concept_columns:
            result_df[col] = result_df_concepts[col]

        # Save to output file
        output_file = "processed_source/aalsdxfx--observation.csv"
        result_df.to_csv(output_file, index=False)
        logging.info(
            f"Successfully saved {len(result_df)} observations to {output_file}"
        )

        return result_df

    except Exception as e:
        logging.error(f"Error processing AALSDXFX data: {str(e)}")
        raise


if __name__ == "__main__":
    # Set index date (example: 2016-01-01)
    index_date = datetime(2016, 1, 1)

    # Process the data
    source_file = "source_tables/aalsdxfx.csv"
    process_aalsdxfx_to_observation(source_file, index_date)
