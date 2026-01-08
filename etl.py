import pandas as pd 
import hopsworks
import requests
import os


def extract_data():

    token=os.getenv("WAQI_API_TOKEN")
    url=f"https://api.waqi.info/feed/A544966/?token={token}"
    response=requests.get(url)
    data=response.json()
    return data

def transofrm_data(data):
    h=data['data']['iaqi']['h']['v']
    pm=data['data']['iaqi']['pm1']['v']
    pm10=data['data']['iaqi']['pm10']['v']
    pm25=data['data']['iaqi']['pm25']['v']
    temp=data['data']['iaqi']['t']['v']
    aqi=data['data']['aqi']
    aqi_idx=data['data']['idx']
    time=data['data']['time']['s']
    aqi_values_list=[h,pm,pm10,pm25,temp,aqi,aqi_idx,time]
    aqi_titles=['humidity','pm1','pm10','pm25','tempertaure','aqi','aqi_index','time']

    aqi_df=pd.DataFrame(dict(zip(aqi_titles,aqi_values_list)),index=[0])
    return aqi_df.to_dict()

def load_data(df_dict):
    df=pd.DataFrame(df_dict)
    project =hopsworks.login()
    fs=project.get_feature_store()
    fg = fs.get_or_create_feature_group(
     name="karachi_air_quality_",
     version=1,
     primary_key=["aqi"],
     description="Air Quality Index data")
    
    fg.insert(df)

extract=extract_data()
transform=transofrm_data(extract)
load_data(transform)


