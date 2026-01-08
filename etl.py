import pandas as pd 
import hopsworks
import requests


def extract_data(url):
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
     name="air_quality_data",
     version=1,
     primary_key=["aqi"],
     description="Air Quality Index data")
    
    fg.insert(df)

extract=extract_data("https://api.waqi.info/feed/A544966/?token=2f610c2eea3409a00f590d97ceda3a5e7c6ae09f")
transform=transofrm_data(extract_data)
load_data()


