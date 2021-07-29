'''
author@ Rohit Sharma
Date : 11-feb-2021
Purpose: 
  - This script converts 1x1 Grid forecast data into 0.5x0.5 grid forecast data using CDO library for min max temperature.
  - We have performed IDW interpolation technique.

Command line arguments:
  - path where grid data files are stored
  - date in "yyyymmdd" format
  - start date format is like "yyyy-mm-ddThh:mm:ss"

Generates following csv & json files:
  - generates data files for each date, separately for all coordinates of following cols: latitude, 
      longitude, ind_lat_lon_05, obs_date, temp_max, temp_min.

Assumptions:
  - indlatlon05grid.csv file present in path where grid data files are stored having lat,lon,0/1 as per 0.5x0.5 grid
  - my_grid file is also present in the same path 
  - splitted min/max files are stored in min_temp and max_temp folder inside the path
  - final output is stored in temp_data folder inside the path 

How to Run: - for 24-feb-2021 bias_crctd files
  - python3 CDO_script.py /home/rohit/Documents/bias_20210224/ 20210224 20210225 2021-02-25T00:00:00

'''
import re
import os
import sys
import time
import json
import glob
import shutil
import requests
import datetime
from cdo import *
import numpy as np
from os import path
import pandas as pd
import xarray as xr
import netCDF4 as nc
from datetime import timedelta
from datetime import datetime

#initialise CDO
cdo = Cdo()
cdo.debug = True
#path where the data is stored
inpath = str(sys.argv[1]) # I/P path where .dat and .ctl files are stored
path = str(sys.argv[2]) #path '/home/rohit/Documents/bias_20210224/' where ind_lat_lon0.5 and my_grid files and all day01.nc etc files present.
oppath = str(sys.argv[3]) #O/P path
temp_data_path = oppath #where the final '.csv' and 'json' files are stored.
df_lat_lon_india = pd.read_csv(path +'ind_lat_lon_05.csv') #read 'india_lat_lon file'
#dates
date = str(sys.argv[4]) #the date is over the file (yyyymmdd)
next_date = str(sys.argv[5]) # date after the original date(yyyymmdd)

#dates to select the date range
start_date = str(sys.argv[6]) #type date in '%Y-%m-%dT%H:%M:%S' format
#end date
start_date_time = datetime.strptime(start_date, "%Y-%m-%dT%H:%M:%S") 
add_dates = start_date_time + timedelta(days=30)
format_time = add_dates.strftime("%Y-%m-%dT%H:%M:%S")
end_date = str(format_time) # it is the end date

#input of ".ctl" file
ctl_max = inpath + "tmax_biascrct_" + date + "00.ctl"
ctl_min = inpath + "tmin_biascrct_" + date + "00.ctl"
#output in ".nc" format
fmax_out = path + "tmax_biascrct_" + date + "00.nc"
fmin_out = path + "tmin_biascrct_" + date + "00.nc"
#save the files into "max_temp" folder and "min_temp" folder
split_max = path + 'max_temp/'
split_min = path +'min_temp/'

#CDO commands
#converting .ctl file into .nc(netCDF) format
cdo.import_binary(input=ctl_max, output=path+"nc_max.nc", options='-f nc')
cdo.import_binary(input=ctl_min, output=path+"nc_min.nc", options='-f nc')
#check info 
cdo.info(input=path+"nc_max.nc")
cdo.info(input=path+"nc_min.nc")
#show grides
cdo.griddes(input=path+"nc_max.nc")
cdo.griddes(input=path+"nc_min.nc")
#set dates
cdo.settaxis(next_date, input=path+"nc_max.nc", output=path+"nc_max_out.nc", options='-a copy')
cdo.settaxis(next_date, input=path+"nc_min.nc", output=path+"nc_min_out.nc", options='-a copy')
cdo.info(input='nc_max_out.nc')
#apply IDW interpolation
cdo.remapdis("my_grid", input=path+"nc_max_out.nc", output="interp_max_result.nc")
cdo.remapdis("my_grid", input=path+"nc_min_out.nc", output="interp_min_result.nc")
#select date range
cdo.seldate(start_date+','+end_date, input="interp_max_result.nc", output=fmax_out)
cdo.seldate(start_date+','+end_date, input="interp_min_result.nc", output=fmin_out)
#get date wise data
cdo.splitday(input= "tmax_biascrct_" + date + "00.nc day", output= "")
#convert ".nc" to ".csv" format for max_temp
all_files = glob.glob(path + "/*.nc")
for fname in all_files:
    df = xr.open_dataset(fname ,decode_times=False)
    df = df.to_dataframe() 
    iname = os.path.splitext(fname)[0]
    df.to_csv(iname + '.csv')
    
#move files from path to split_min/max folders
#files = ['day01.csv', 'day02.csv','day03.csv','day04.csv','day05.csv','day06.csv','day07.csv','day08.csv','day09.csv','day10.csv','day11.csv','day12.csv','day13.csv','day14.csv','day15.csv','day16.csv','day17.csv','day18.csv','day19.csv','day20.csv','day21.csv','day22.csv','day23.csv','day24.csv','day25.csv','day26.csv','day27.csv','day28.csv']
max_files = glob.glob("day*.csv")
print("Maximum Files: ", max_files)    
for f in max_files:   
     new_path = shutil.move(f"{path}/{f}", os.path.join(split_max,f))
     #shutil.move(os.path.join(src, filename), os.path.join(dst, filename)
     print("max_new_path: ", new_path) 
#cdo command to split file
cdo.splitday(input= "tmin_biascrct_" + date + "00.nc day", output= "")

#convert ".nc" to ".csv" format for min_temp
all_files = glob.glob(path + "/*.nc")
for fname in all_files:
    df = xr.open_dataset(fname ,decode_times=False)
    df = df.to_dataframe() 
    iname = os.path.splitext(fname)[0]
    df.to_csv(iname + '.csv')
    
#files = ['day01.csv', 'day02.csv','day03.csv','day04.csv','day05.csv','day06.csv','day07.csv','day08.csv','day09.csv','day10.csv','day11.csv','day12.csv','day13.csv','day14.csv','day15.csv','day16.csv','day17.csv','day18.csv','day19.csv','day20.csv','day21.csv','day22.csv','day23.csv','day24.csv','day25.csv','day26.csv','day27.csv','day28.csv']
min_files = glob.glob("day*.csv")
for f in min_files:
     new_path = shutil.move(f"{path}/{f}", os.path.join(split_min,f))
     print("min_new_path: ", new_path) 

#Merging of data min_temp, max_temp & india_lat_lon
maxx = split_max
minn = split_min

for i in range(1, 32): 
    print(i)
    max_data = maxx + "day" + str(i).zfill(2) +".csv"
    min_data = minn + "day" + str(i).zfill(2) +".csv"
    
    data_max = pd.read_csv(max_data)
    
    data_max["tmax"].fillna("99.9", inplace = True)
    df_max = pd.DataFrame(columns = ["latitude","longitude","max_temp","obs_date"])
    df_max = pd.DataFrame(columns = ["latitude", "longitude","max_temp"])
    df_max["latitude"] = data_max["lat"]
    df_max["longitude"] = data_max["lon"]
    df_max["max_temp"] = data_max["tmax"]
    df_max["obs_date"] = pd.to_datetime(data_max["time"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    print("df max obsdate: ", df_max["obs_date"])
     
    data_min = pd.read_csv(min_data)
    
    data_min["tmin"].fillna("99.9", inplace = True)
    df_min = pd.DataFrame(columns = ["latitude", "longitude","min_temp","obs_date"])
    df_min = pd.DataFrame(columns = ["latitude", "longitude","min_temp"])
    df_min["latitude"] = data_min["lat"]
    df_min["longitude"] = data_min["lon"]
    df_min["min_temp"] = data_min["tmin"]
    df_min["obs_date"] = pd.to_datetime(data_min["time"], format="%Y%m%d").dt.strftime("%Y-%m-%d")
    print("df min obsdate: ", df_min["obs_date"])
    
    temp_data = pd.merge(df_max, df_min, on=["latitude", "longitude", "obs_date"])
    
    #initialize out dataframe
    cols = ["obs_date", "latitude", "longitude", "india_lat_lon", "max_temp", "min_temp"]
    df_out = pd.DataFrame(columns=cols)
    df_out_india_inf = pd.DataFrame(columns=cols)
    
    df_out_india_inf = pd.merge(temp_data, df_lat_lon_india, on=['latitude', 'longitude'], how='right')  
    #df_out_india_inf = pd.merge(df_out, df_lat_lon_india, on=["latitude", "longitude"], left_index=True, how="left") 
    df_out_india = df_out_india_inf[df_out_india_inf["india_lat_lon"] > 0]
    print("Out INDIA: ",df_out_india)
  
    #df_out_india["obs_date"] = pd.to_datetime(df_out_india["obs_date"], format='%Y%m%d')
    #df_out_india.loc[:, "obs_date"] = pd.to_datetime(df_out_india["obs_date"], format='%Y%m%d')
    print("OBS_DATE: ", df_out_india["obs_date"])
    
    df_india = pd.DataFrame(columns = ["latitude", "longitude","max_temp","min_temp","obs_date", "india_lat_lon"])
    df_india = pd.DataFrame(columns = ["latitude", "longitude","max_temp","min_temp", "india_lat_lon"])
    df_india["latitude"] = df_out_india["latitude"]
    df_india["longitude"] = df_out_india["longitude"]
    df_india["max_temp"] = df_out_india["max_temp"]
    df_india["min_temp"] = df_out_india["min_temp"]
    df_india["obs_date"] = df_out_india["obs_date"]
    df_india["india_lat_lon"] = df_out_india["india_lat_lon"]
    
    #convert min/max_temp upto two decimal place                                              
    arr1 = np.round(df_india['max_temp'].astype(float), 2)
    arr2 = np.round(df_india['min_temp'].astype(float), 2)
    df_india["max_temp"] = arr1
    df_india["min_temp"] = arr2
    
    date = df_india['obs_date'].values[1]
    print("date: ", date)
    #print("temp_data: ", temp_data_path)
    
    filepathcsv = temp_data_path + 'temp_india' + date + '.csv'
    filepathjson = temp_data_path + "temp_india" + date +".json"

#get the final output as in csv and json format     
    df_india.to_csv(filepathcsv, index = False)  
    df_india.to_json(filepathjson, orient = 'records')    
