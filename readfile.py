import pandas as pd

def read_file():
    df_source = pd.read_csv("/Users/manjukb/Desktop/ETL_DataBase/File01.csv")
    return df_source

df=read_file()
