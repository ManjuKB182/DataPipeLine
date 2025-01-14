import pandas as pd
import matplotlib.pyplot as plt


df_sprint = pd.read_csv("/Users/manjukb/Desktop/ACL2025/Sprint_data_upd.csv")


#print(df_sprint.columns)
df_sprint['Batch_Group'] = df_sprint['ಬ್ಯಾಚ್'].astype(int).apply(lambda x: 'A' if 1 <= x <= 14 else ('B' if 15 <= x <= 23 else 'C'))

# Group by 'Batch_Group' and 'ಲಿಂಗ', then count the occurrences
New_df = df_sprint.groupby(['Batch_Group', 'ಲಿಂಗ:']).size().reset_index(name='Counts')

#Registn_Report = New_df.style.set_properties(**{'text-align': 'center'}).hide(axis="index")

pd.set_option('display.colheader_justify','Center')

colors = ['green' if Batch_Group == 'A'  else( 'Red' if Batch_Group == 'B' else 'Yellow' ) for Batch_Group in New_df['Batch_Group']]

New_df.plot(x='Batch_Group', y='Counts', kind='bar', color= colors)
plt.title('SPRINT EVENT REGISTRATION')
plt.xlabel('Groups')
plt.ylabel('Counts')
plt.show()

print('\nSPRINT EVENT REGISTRATION\n\n',New_df)


