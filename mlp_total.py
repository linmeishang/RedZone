
# This is the code of training a MLP
#%%
from math import fabs
from xml.sax.xmlreader import InputSource
import numpy as np
import random as rn
import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
import pickle
from pickle import load
from pickle import dump
from datetime import datetime
import time
import tensorflow as tf
import tensorflow.keras
from tensorflow.keras import layers
from tensorflow.keras.models import Model
from tensorflow.keras.layers import Dense, Input, Dropout, Activation, Lambda
from tensorflow.keras.losses import mean_squared_error, binary_crossentropy
from tensorflow.keras.callbacks import ModelCheckpoint, EarlyStopping, CSVLogger
from tensorflow.keras.layers.experimental.preprocessing import Normalization, Rescaling
from tensorflow.keras.callbacks import Callback
from tensorflow.keras import layers
from tensorflow.keras import optimizers
from tensorflow.keras import backend as K
from tensorflow.keras.models import load_model
# K.tensorflow_backend._get_available_gpus()
from scipy import stats
from sklearn.metrics import r2_score
from sklearn.preprocessing import MinMaxScaler
import itertools
import random


#%%
# find the latest DataPreparation
path = r'N:\agpo\work2\MindStep\WP_4\WP4_Task_4_5\SurrogateABM\DataPreparation'

all_folders = glob.glob(os.path.join(path + '/DataPreparationTotal*/'))

# find the latest Train Data
DataPreparation = max(all_folders, key=os.path.getctime)

# set DataPreparation as work dir
path = DataPreparation
os.chdir(path)
print("Current Working Directory " , os.getcwd())

#%%
# load the raw data into DataFrame
X_train_raw = pd.read_parquet('X_train_raw.parquet.gzip') # used for training
X_test_raw = pd.read_parquet('X_test_raw.parquet.gzip') # used for testing

Y_train_raw = pd.read_parquet('Y_train_raw.parquet.gzip') # used for giving yhat_train_raw column names and calculate R2 between yhat_train_raw and Y_train_raw
Y_test_raw = pd.read_parquet('Y_test_raw.parquet.gzip') # used for giving yhat_test_raw column names and calculate R2 between yhat_test_raw and Y_test_raw

Y_train = pd.read_parquet('Y_train.parquet.gzip') # used for training
Y_test = pd.read_parquet('Y_test.parquet.gzip') # used for testing


print('shape of X_train_raw:', X_train_raw.shape)
print('shape of Y_train_raw:', Y_train_raw.shape)
print('shape of X_test_raw:', X_test_raw.shape)
print('shape of Y_test_raw:', Y_test_raw.shape)

#%%
X_train_raw_base = pd.read_parquet('X_train_raw_base.parquet.gzip')
X_train_raw_red = pd.read_parquet('X_train_raw_red.parquet.gzip')
Y_train_raw_base = pd.read_parquet('Y_train_raw_base.parquet.gzip')
Y_train_raw_red = pd.read_parquet('Y_train_raw_red.parquet.gzip')

X_test_raw_base = pd.read_parquet('X_test_raw_base.parquet.gzip')
X_test_raw_red = pd.read_parquet('X_test_raw_red.parquet.gzip')
Y_test_raw_base = pd.read_parquet('Y_test_raw_base.parquet.gzip')
Y_test_raw_red = pd.read_parquet('Y_test_raw_red.parquet.gzip')

#%%
# load the scalers
with open('X_scaler.pkl', 'rb') as file:
    X_scaler = pickle.load(file)
with open('Y_scaler.pkl', 'rb') as file:
    Y_scaler = pickle.load(file)
    
print(X_scaler, Y_scaler)
print('Both scalers are loaded.')


#%%
import itertools
# 32, 64, 128, 256, 512,
# 32, 64, 128, 256, 
list1 = [128] #, 64, 128, 256, 512, 1024]
list2 = [256] #, 64, 128, 256, 512, 1024]

    
# lr_list = [0.0001, 0.0003, 0.001, 0.003, 0.01]
# m_list = [16, 32, 64, 128]
# opt_list = ["Adam", "Adamax", "RMSprop", "SGD"]

#for i in list1:
for i, p in itertools.product(list1, list2):
    layer_1, layer_2, layer_3, layer_4 = i, p, 128, 0
    lr = 0.001
    m = 32
    n_epoch = 200
    decay_rate = lr/n_epoch
    optimizer = tf.keras.optimizers.Adam(learning_rate=lr, decay=decay_rate)

    # set a new working directory and save the model and all related results
    model_path = path+"Model"+ datetime.now().strftime("_%Y%m%d%H%M") 

    try:
        os.makedirs(model_path)
    except OSError:
        print ("Creation of the directory %s failed" % model_path)
    else:
        print ("Successfully created the directory %s" % model_path)

    # change working directory in order to save all data later
    os.chdir(model_path)
    print("Current Working Directory " , os.getcwd())

    model_name = str(model_path[-18:])
    print(model_name)

    ###############################################################################################
    # Neural network layers including preprocessing layer
    inputs = tf.keras.Input(shape = X_train_raw.shape[1])

    # Preprocessing layer
    x = tf.keras.layers.Lambda(lambda x: (x - X_scaler.data_min_)/ tf.where(
    X_scaler.data_max_ > X_scaler.data_min_, x=tf.cast(X_scaler.data_max_ - X_scaler.data_min_, dtype=tf.float32), y=tf.broadcast_to(tf.constant(1, dtype=tf.float32), X_scaler.data_max_.shape)
    ))(inputs)

    # 1st layer
    
    x = tf.keras.layers.Dense(layer_1, activation='sigmoid') (x)
    x = tf.keras.layers.BatchNormalization()(x)
    # 2nd layer
    if layer_2 != 0:
        x = tf.keras.layers.Dense(layer_2, activation = 'sigmoid')(x)
        x = tf.keras.layers.BatchNormalization()(x)

    else: 
        None

    # 3rd layer
    if layer_3 != 0:
        x = tf.keras.layers.Dense(layer_3, activation = 'sigmoid')(x)
        x = tf.keras.layers.BatchNormalization()(x)
    else: 
        None
        

    # 4th layer
    if layer_4 != 0:
        x = tf.keras.layers.Dense(layer_4, activation = 'sigmoid')(x)
        x = tf.keras.layers.BatchNormalization()(x)
    else: 
        None


    outputs = tf.keras.layers.Dense(Y_train.shape[1], activation = 'relu') (x) # Y_train is already scaled before the model
    model = tf.keras.Model(inputs, outputs)
    model.summary()
    number_parameters = model.count_params()


    # Compile and train the model
    model.compile(optimizer= optimizer, loss='mean_squared_error')

    # patient early stopping
    es = EarlyStopping(monitor='val_loss', mode='min', verbose=1, patience=20)
    mc = ModelCheckpoint('best_model.h5', monitor='val_loss', mode='min', verbose=1, save_weights_only = False, save_best_only=True)
    csv_logger = CSVLogger('training.log')


    start_time = time.time()
    # Fit the model: use the raw data of X_train_raw, but Y_train is scaled data
    my_history = model.fit(x = X_train_raw, y = Y_train, 
            epochs = n_epoch, batch_size = m, shuffle=True, validation_split=0.1, verbose=2)
        
    # my_history = model.fit(x = X_train_raw, y = Y_train, 
    #         epochs = n_epoch, batch_size = m, shuffle=True, validation_split=0.1, verbose=2, 
    #         callbacks=[es, mc, csv_logger])
    end_time = time.time()

    time_to_stop =  end_time - start_time
    stopped_epoch = es.stopped_epoch

    # save the model
    model.save('my_model.h5')  # creates a HDF5 file 'my_model.h5'

    # Summarize history for loss
    # print(my_history.history)
    # history = my_history.history
    # plt.plot(history['loss'])
    # plt.plot(history['val_loss'])
    # plt.title('model loss')
    # plt.ylabel('loss')
    # plt.xlabel('epoch')
    # plt.legend(['training', 'validation'], loc='upper right')
    # plt.savefig('./fig_loss.png')
    # plt.show()


    # load the model
    best_model = load_model('my_model.h5')
    print(best_model.summary())



    # Evaluate model performance using the best_model.h5
    train_score = best_model.evaluate(x = X_train_raw, y = Y_train, batch_size=m, verbose=0)
    print("train_score = ", train_score)
    # train_score contains 3 losses
    test_score = best_model.evaluate(x = X_test_raw, y = Y_test, batch_size=m, verbose=0)
    print("test_score = ", test_score)


    # Precit for train and test dataset
    yhat_train = best_model.predict(X_train_raw)
    yhat_train = pd.DataFrame(yhat_train, columns = Y_train.columns)
    #yhat_train.to_parquet('yhat_train.parquet.gzip', compression='gzip')


    start_time = time.time()
    yhat_test = best_model.predict(X_test_raw)
    end_time = time.time()
    time_cost_test =  end_time - start_time

    yhat_test = pd.DataFrame(yhat_test, columns = Y_test.columns)
    #yhat_test.to_parquet('yhat_test.parquet.gzip', compression='gzip')


    # r2 of train dataset without scaling back to raw values
    r2_train = r2_score(Y_train, yhat_train)
    print("r2 of training set", r2_train)

    # r2 for test dataset
    r2_test = r2_score(Y_test, yhat_test)
    print("r2 of test set", r2_test)


    # R2 for each targets for test dataset
    test_r2_dic = {}

    for k, j in zip(range(0,len(Y_test.columns)), Y_test.columns): 

        y_true = Y_test.iloc[:,k]

        y_pred = yhat_test.iloc[:,k]

        r2 = r2_score(y_true,y_pred)

        test_r2_dic[j] = r2 


    # store hyperparameters into a list
    hyperparameters_dic = {"layer_1": layer_1, 
                        "layer_2": layer_2, 
                        "layer_3": layer_3, 
                        "layer_4": layer_4, 
                        "train_size": X_train_raw.shape[0],
                        "test_size": X_test_raw.shape[0],
                        "learning_rate": lr,
                        "minibatch_size": m,
                        "epoch": n_epoch, 
                        "stopped_epoch": stopped_epoch,
                        "optimizer": optimizer, 
                        "number_parameters": number_parameters,
                        "time_to_stop": time_to_stop, 
                        "time_cost_test": time_cost_test} 


    # store overall indicators into a list
    train_dic = {"r2_train": r2_train, "r2_test": r2_test} 


    # Combine all dictionaries that we want to store
    result_dic = {**hyperparameters_dic, **train_dic,  **test_r2_dic}

    # Append this dictionary to an excel in model assessment
    df = pd.DataFrame(data=result_dic, index=[model_name])
    df = (df.T)
    print (df)
    df.to_excel(model_name+".xlsx")



    # Construct FarmLin using the trained best model by adding a reverse-scaling layer for predictions
    FarmLin = tf.keras.Sequential()
    FarmLin.add(best_model)
    FarmLin.add(Lambda(lambda x: x * (Y_scaler.data_max_- Y_scaler.data_min_) + Y_scaler.data_min_)) # add a reverse-scaling
    FarmLin.add(Lambda(lambda x: tf.concat([x[:, 0:1], tf.math.round(x[:, 1:58]), x[:, 58:188], tf.math.round(x[:, 188:202]), x[:, 202:]], axis = 1))) # add the round up layer
    FarmLin.add(Lambda(lambda x: tf.concat([x[:, 0:1], tf.maximum(x[:, 1:212], 0), x[:, 212:]], axis = 1))) # add a positive layer for output 1-211
    FarmLin.summary()
    FarmLin.save('FarmLin.h5')
    FarmLin= load_model('FarmLin.h5')
    print(FarmLin.summary())




    # Employ FarmLin
    # Precit for train and test dataset: FarmLin produces numpy.ndarray
    yhat_train_raw = FarmLin.predict(X_train_raw)
    yhat_test_raw = FarmLin.predict(X_test_raw)


    yhat_train_raw = pd.DataFrame(yhat_train_raw, columns = Y_train.columns)
    # yhat_train_raw.to_parquet('yhat_train_raw.parquet.gzip', compression='gzip')

    yhat_test_raw = pd.DataFrame(yhat_test_raw, columns = Y_test.columns)
    # yhat_test_raw.to_parquet('yhat_test_raw.parquet.gzip', compression='gzip')


    # yhat_test_raw.hist(bins=30, figsize=(100, 100))


    # r2 of train dataset 
    r2_train = r2_score(Y_train_raw, yhat_train_raw)
    print("r2 of training set", r2_train)

    # r2 for test dataset
    r2_test = r2_score(Y_test_raw, yhat_test_raw)
    print("r2 of test set", r2_test)


    # R2 for each targets for test dataset
    test_r2_dic = {}

    for k, j in zip(range(0,len(Y_test_raw.columns)), Y_test_raw.columns): 

        y_true = Y_test_raw.iloc[:,k]

        y_pred = yhat_test_raw.iloc[:,k]

        r2 = r2_score(y_true,y_pred)

        test_r2_dic[j] = r2 



    yhat_train_raw_base = FarmLin.predict(X_train_raw_base)
    yhat_train_raw_red = FarmLin.predict(X_train_raw_red)

    yhat_test_raw_base = FarmLin.predict(X_test_raw_base)
    yhat_test_raw_red = FarmLin.predict(X_test_raw_red)

    # r2 of train dataset without scaling back to raw values
    r2_train_base = r2_score(Y_train_raw_base, yhat_train_raw_base)
    r2_train_red = r2_score(Y_train_raw_red, yhat_train_raw_red)
    print("r2_train_base: ", r2_train_base)
    print("r2_train_red: ", r2_train_red)

    # r2 for test dataset
    r2_test_base = r2_score(Y_test_raw_base, yhat_test_raw_base)
    r2_test_red = r2_score(Y_test_raw_red, yhat_test_raw_red)
    print("r2_test_base: ", r2_test_base)
    print("r2_test_red: ", r2_test_red)


    # store hyperparameters into a list
    hyperparameters_dic = {"layer_1": layer_1, 
                        "layer_2": layer_2, 
                        "layer_3": layer_3, 
                        "layer_4": layer_4, 
                        "train_size": X_train_raw.shape[0],
                        "test_size": X_test_raw.shape[0],
                        "learning_rate": lr,
                        "minibatch_size": m,
                        "epoch": n_epoch, 
                        "stopped_epoch": stopped_epoch,
                        "optimizer": optimizer, 
                        "number_parameters": number_parameters,
                        "time_to_stop": time_to_stop, 
                        "time_cost_test": time_cost_test} 


    # store overall indicators into a list
    train_dic = {"r2_train": r2_train, "r2_test": r2_test, \
                "r2_train_base": r2_train_base, "r2_train_red": r2_train_red, \
                    "r2_test_base": r2_test_base, "r2_test_red": r2_test_red} 

    # Combine all dictionaries that we want to store
    result_dic = {**hyperparameters_dic, **train_dic,  **test_r2_dic}

    # Append this dictionary to an excel in model assessment
    df = pd.DataFrame(data=result_dic, index=["FarmLin_"+model_name])
    df = (df.T)
    print (df)
    df.to_excel("FarmLin_"+model_name+".xlsx")


#%%


