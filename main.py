from datetime import datetime
import requests
import json
import pandas as pd
from google.cloud import storage
import google.cloud.logging
import logging

def get_data_api(url):
    
    # create lists
    code = []
    name = []
    short_name = []
    strength = []
    strength_overall_home = []
    strength_overall_away = []
    win = []

    try:
        # send API call and retrieve JSON data
        response = requests.get(url)
        # convert Json to a list
        data = json.loads(response.text)

        for i in data['teams']:
            code.append(i['code'])
            name.append(i['name'])
            short_name.append(i['short_name'])
            strength.append(i['strength'])
            strength_overall_home.append(i['strength_overall_home'])
            strength_overall_away.append(i['strength_overall_away'])
            win.append(i['win'])
        
        # Prepare a dictionary in order to turn it into a pandas dataframe
        teams_dict = {
            "code" : code,
            "name": name,
            "short_name" : short_name,
            "strength" : strength,
            "strength_overall_home" : strength_overall_home,
            "strength_overall_away" : strength_overall_away,
            "win" : win
        }
    
    except Exception as e:
        print("Error to get data from API")
        exit(1)
    
    # create Pandas dataframe to structure data
    df_teams = pd.DataFrame(teams_dict,columns=['code',
                                                'name',
                                                'short_name',
                                                'strength',
                                                'strength_overall_home',
                                                'strength_overall_away',
                                                'win'
                                                ])

    return df_teams

def api_to_gcs(df,filename,project_name,bucket_name):

    # setup project and bucket in GCP
    client = storage.Client(project=project_name)
    bucket = client.get_bucket(bucket_name)

    # create a blob with Data Frame records
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(index = False),content_type = 'csv')

def main(data, context):
    
    # set variables
    url = 'https://fantasy.premierleague.com/api/'
    endpoint = 'bootstrap-static/'
    filename = datetime.now().strftime("%Y%m%d%H%M%S")+'_return_api_001.csv'
    project_name = 'my-project-etl-bq-dataproc'
    bucket_name = '20230908_my_bucket_001'
    
    # call get_data
    logging.info(datetime.now().strftime("%Y%m%d%H%M%S")+' - Cloud function started')
    df = get_data_api(f'{url}{endpoint}')
    # call api_to_gcs
    df = api_to_gcs(df,filename,project_name,bucket_name)
    logging.info(datetime.now().strftime("%Y%m%d%H%M%S")+' - Cloud function finished')