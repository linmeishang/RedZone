# open gdx file in python and automatically add read folders and add it to exitsing dataframe 
# must type in the command "conda activate py36" (might also work in python 3.7 environment)

#%%
import os
import gdxpds
import pandas as pd
from gdxpds import gdx
from collections import OrderedDict
import numpy as np
import glob
import os
import time
from datetime import datetime
import pickle

# cd /d d:\temp

#%%
# mapperInstance is a function to rename columns so that we can select them
def mapperInstance():

    def gen_i():
        i = 0
        while True:
            yield str(i)
            i = i+1
    gen = gen_i()

    def mapper(s):
        return (s + gen.__next__())
    
    return mapper
    
#%%

# get_df is a function to read a single gdx file of a single draw, and transform it into a data frame
def get_df(gdx_file):
   
    try:
        #print(f"Reading file: {gdx_file}")
        p_res = gdxpds.read_gdx.to_dataframe(gdx_file, symbol_name='p_sumFarmLin', gams_dir= r"N:\\soft\\GAMS39", old_interface=False)
        
        if p_res.empty:
            print(f"Warning: No data read from {gdx_file}")
            return pd.DataFrame()  # Return empty df
        
    except Exception as e:
        print(f"Error reading {gdx_file}: {e}")
        return pd.DataFrame()
    
    
    # print(p_res)

    # Rename Columns (1st pass)
    mpr = mapperInstance()
    
    p_res.rename(mpr, axis="columns", inplace=True)
    #print('here is p_res 1:', p_res)
    
    # p_res = p_res.drop(['*4'], axis=1) # drop the column *4
    # add a column and give the new column a name (combine all the names before)
    p_res['concat']= pd.Series(p_res[['*2', '*3', '*4']].fillna('').values.tolist()).map(lambda x: '_'.join(map(str,x)))
    #print('here is p_res 2:', p_res)
    
    df = p_res[['Value6','concat']].T # change 5 to 6
    #print('here is p_res 3:', p_res)
    
    # print(df)
    df = df.rename(columns=df.iloc[1])
    #print("renamed df:", df)
    
    df = df.drop(['concat']) # drop the row "concat"
    #print(df.columns)
    
    df = df.loc[:,~df.columns.duplicated()]
    #print("final df:", df)    

    return df

# %%
# get_df_parquet is function to read all gdx files in a folder, concate them and store in as a parquet file
def get_df_parquet(folder):

    all_files = glob.glob(os.path.join(folder + "\\*.gdx"))
    
    df = pd.concat([get_df(gdx_file) for gdx_file in all_files])
   
    print(df)
    
    df.index = [f'draw_{i}' for i in range(df.shape[0])]

    df.to_parquet("N:\\agpo\\work2\\MindStep\\WP_4\\WP4_Task_4_5\\SurrogateABM\\DataCollectionBase\\"+folder[len(folder)-9:len(folder)-1]+".parquet.gzip",  compression="gzip")
   
    print('file saved')
    return df


# %%
# Get all folders in DataCollection
path = r'N:\agpo\work2\MindStep\WP_4\WP4_Task_4_5\SurrogateABM\DataCollectionBase'
os.chdir(path) 

all_folders = glob.glob(os.path.join(path + '/2025*/'))
print("all_folders:", all_folders)

#%%
# python finds the latest .parquet.gzip
all_parquets = glob.glob(os.path.join(path+"\\total_df_Baseline_2025*.parquet.gzip"))

print(all_parquets)


if len(all_parquets) == 0:

    print("No total_df yet")

    total_df = pd.DataFrame()

else: 

    total_df = max(all_parquets, key=os.path.getctime)

    total_df = pd.read_parquet(total_df) 

    
print ("Latest total_df is:", total_df)

print("Shape of the latest total_df:", total_df.shape)


#%%

for folder in all_folders:

    filename = folder[len(folder)-9:len(folder)-1]
    
    print(filename)
    
    # if .parquet.gzip exist, do nothing; if not, creat a parquet file by get_df_parquet
    if os.path.isfile(filename+".parquet.gzip"):
        
        print(filename, "File exist")
        
        #df = pd.read_parquet(filename+".parquet.gzip") 
        
        #total_df = pd.concat([total_df, df], axis=0)
        
    else:
        print(filename, "File not exist")

        df = get_df_parquet(folder)
        
        print("df:", df)
        # append it into total_df
        total_df = pd.concat([total_df, df], axis=0)
       

        print(filename, "File is created")


#%%
# Rename the indexs of total_df
total_df.index = [f'draw_{i}' for i in range(total_df.shape[0])]

print("shape of total_df:", total_df.shape)
print("Total df: ", total_df)



#%%
# Rename total_df according to time YYMMDD
Date = datetime.now().strftime("%Y%m%d") # use ("%Y%m%d-%H%M%S") for hour-minute-second

total_df.to_parquet("N:\\agpo\\work2\\MindStep\\WP_4\\WP4_Task_4_5\\SurrogateABM\\DataCollectionBase\\total_df_Baseline_"+Date+".parquet.gzip",  compression="gzip")

print("new total_df saved")


#%%
# path = r'N:\agpo\work2\MindStep\WP_4\WP4_Task_4_5\SurrogateABM\DataCollectionBase'
# os.chdir(path)
# # save total_df in excel
# print(total_df)

#%%
#total_df.to_excel('test_500.xlsx')


#%%
# Get the name of columns so that we can define it later
# ColumnNames = list(total_df.columns)
# print(ColumnNames)
# with open('ColumnNames.pkl', 'wb') as f:
# pickle.dump(ColumnNames, f)

