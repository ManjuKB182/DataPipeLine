import pandas as pd

df_sprint = pd.read_csv("/Users/manjukb/Desktop/ACL2025/Sprint_data_upd.csv")


#print(df_sprint.columns)
df_sprint['Batch_Group'] = df_sprint['ಬ್ಯಾಚ್'].astype(int).apply(lambda x: 'A' if 1 <= x <= 14 else ('B' if 15 <= x <= 23 else 'C'))

# Group by 'Batch_Group' and 'ಲಿಂಗ', then count the occurrences
New_df = df_sprint.groupby(['Batch_Group', 'ಲಿಂಗ:']).size().reset_index(name='Counts')

Registn_Report = New_df.style.set_properties(**{'text-align': 'center'}).hide(axis="index")

pd.set_option('display.colheader_justify','Center')

print('\nSPRINT EVENT REGISTRATION\n\n',Registn_Report)



