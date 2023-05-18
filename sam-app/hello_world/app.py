import json
import requests
import boto3
import pandas as pd
from pytz import timezone
import datetime


def lambda_handler(event, context):

    ssm_client = boto3.client('ssm')

    # Retrieve the secret value using the parameter name
    response = ssm_client.get_parameter(Name='dukascopy-key', WithDecryption=True)
    api_key = response['Parameter']['Value']

    api_url = "https://freeserv.dukascopy.com/2.0/?path=api/historicalPrices"

    instrument = "2024"
    timeFrame = "1min"


    response = requests.get(api_url, params={'key': api_key,
                                                'instrument': instrument,
                                                'timeFrame': timeFrame,
                                                'count': 5000,
                                                'offerSide': 'B'})

    if response.status_code == 200:
        response_data = response.json()
        bid_data = response_data['candles']
    else:
        print(f"Error in API request {request_number + 1}: {response.status_code}")

    response = requests.get(api_url, params={'key': api_key,
                                                'instrument': instrument,
                                                'timeFrame': timeFrame,
                                                'count': 5000,
                                                'offerSide': 'A'})

    if response.status_code == 200:
        response_data = response.json()
        ask_data = response_data['candles']
    else:
        print(f"Error in API request {request_number + 1}: {response.status_code}")

    bid_df = pd.DataFrame(bid_data)
    bid_df.index = pd.to_datetime(bid_df.timestamp, unit='ms')

    ask_df = pd.DataFrame(ask_data)
    ask_df.index = pd.to_datetime(ask_df.timestamp, unit='ms')
    ask_df = ask_df.drop(['timestamp'], axis = 1)

    df = bid_df.join(ask_df)

    df.index = pd.to_datetime(df.index)
    df.index = df.index.tz_localize('UTC')
    london = timezone('Europe/London')

    df.index = df.index.tz_convert(london)

    df.index = df.index.strftime('%Y-%m-%d %H:%M:%S')

    df.index = pd.to_datetime(df.index)
    df.index.names = ['date']

    #Date Setup
    current_year = datetime.datetime.now().year
    current_month = datetime.datetime.now().month
    current_day = datetime.datetime.now().day

    #Current Data
    current_file_loc = 's3://fx-data-raw/gbpjpy/oneminute/'+str(current_year)+'_'+str(current_month).zfill(2)+'.parquet'
    try:
        current_data = pd.read_parquet(current_file_loc)
    except:
        current_data = None

    if current_data is None:
        combined_df = df 
    else: 
        combined_df = current_data.append(df)
        combined_df = combined_df[~combined_df.index.duplicated(keep='first')]

    combined_df.to_parquet(current_file_loc)

    return "Done"