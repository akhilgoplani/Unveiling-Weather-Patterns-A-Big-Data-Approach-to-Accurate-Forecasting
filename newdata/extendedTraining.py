# In[ ]:Importing required packages

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from statsmodels.tsa.statespace.sarimax import SARIMAX
from statsmodels.graphics.tsaplots import plot_acf, plot_pacf
import pickle
from sklearn.metrics import mean_squared_error


city_df = spark.read.csv('file:///home/hadoop/Documents/Indian_Cities_Database.csv', header=True)

for city in city_df.collect():      
    # Load the json file
    file_path = 'file:///home/hadoop/Documents'
    data = pd.read_csv(f'{file_path}/newdata/city/{city["City"]}/*.csv')
    
    # In[ ]: dropping unrequired features
    selected_columns = ['Date','T2M']
    data = data.loc[:, selected_columns]

    # In[ ]: convert the date column to datetime format
    data['Date'] = pd.to_datetime(data['Date'], format='%Y%m%d%H')
    data.head()

    # In[ ]: changing index to date
    data.index = data['Date']
    data.head()

    # In[ ]: soring on the basis of date
    data=data.sort_index()

    # In[ ]: Renaming columns
    data.rename(columns={'T2M':'TEMP'}, inplace=True)


	# In[ ]: Loading the saved SARIMA model from the file
	with open(f'/home/hadoop/Documents/city_modal/{city}_model.pkl', 'rb') as f:
    	city_result = pickle.load(f)
    
	# In[ ]: extending the training data
	city_result=city_result.extend(data['TEMP'])

	# In[ ]: saving the extended training data
	with open(f'/home/hadoop/Documents/city_modal/{city}_model.pkl', 'wb') as f:
    	pickle.dump(city_result, f)
