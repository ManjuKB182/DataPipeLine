import pytest
import pandas as pd

#Verify the Duplicate Values in File
def test_check_duplicate():
    target_df = pd.read_csv("NewEarthData.csv",sep=",")
    count_duplicates = target_df.duplicated().sum()
    assert count_duplicates == 0, "Duplicates Found Please check the File"

def test_check_emptyfile():
    target_df = pd.read_csv("NewEarthData.csv",sep=",")
    assert not target_df.empty, "Target File is Empty- Please check the File"


def test_count_cryo_sleep():
    # Load the CSV file
    target_df = pd.read_csv(filepath_or_buffer="NewEarthData.csv", sep=",")

    # Ensure the 'CryoSleep' column exists
    assert "CryoSleep" in target_df.columns, "CryoSleep column not found in the dataset."

    # Count the occurrences of each value in the 'CryoSleep' column
    cryo_sleep_counts = target_df['CryoSleep'].value_counts()

    # Ensure there are exactly two unique values in the 'CryoSleep' column
    assert len(cryo_sleep_counts) == 2, f"Expected 2 unique values in CryoSleep, found {len(cryo_sleep_counts)}."

    # Check if the counts of the two values match
    count_values = cryo_sleep_counts.values
    assert count_values[0] == count_values[1], f"Counts do not match: {count_values[0]} vs {count_values[1]}."
