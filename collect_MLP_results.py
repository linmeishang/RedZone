
#%%
import pandas as pd
import glob

all_files = glob.glob(r'N:\agpo\work2\MindStep\WP_4\WP4_Task_4_5\SurrogateABM\DataPreparation\DataPreparationTotal_202411041643\*2024*\*.xlsx', recursive=True)

print(all_files)

len(all_files)
#%%

df = pd.DataFrame()

#%%
for file in all_files:

    data_from_this_file = pd.read_excel(file)

    # print(data_from_this_file.iloc[:,1])

    df = pd.concat([df, data_from_this_file.iloc[:,1]], axis = 1)
    
print(df)
#%%

df.to_excel(r'N:\agpo\work2\MindStep\WP_4\WP4_Task_4_5\SurrogateABM\DataPreparation\mlp_results_FarmLin_2024 8.xlsx')
# %%
