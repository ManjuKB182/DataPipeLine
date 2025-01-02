import pytest
import pandas as pd

#Verify the Duplicate Values in File

def check_duplicate():
    target_df = pd.read_csv("NewEarthData.csv",sep=",")
    count_duplicates = target_df.duplicated().sum()
    assert count_duplicates == 0, "Duplicates Found Please check the File"

def check_emptyfile():
    target_df = pd.read_csv("NewEarthData.csv",sep=",")
    assert not target_df.empty, "Target File is Empty- Please check the File"