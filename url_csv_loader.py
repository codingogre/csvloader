import base64
import pandas
import sys
import os
import simplejson as json
import hashlib
from datetime import datetime
from iso3166 import countries
from elasticsearch import Elasticsearch
from elasticsearch.helpers import streaming_bulk


ES = Elasticsearch(
     cloud_id="COVID-19:dXMtZWFzdC0xLmF3cy5mb3VuZC5pbyQ0MmRkYWE2NTg4Yjc0NDkxYjU4ZjdhZDhkZTRlZjM0YiQ3Mjg4ODZjNTRiNTA0MjIzOTM0N2NiNjNjZDBkM2YyMw==",
     http_auth=("elastic", "rfjuBlE2v7fpngRyPYfB8qlx")
)
JHU_URL = "https://raw.githubusercontent.com/CSSEGISandData/COVID-19/master/csse_covid_19_data/csse_covid_19_daily_reports/"


def hello_pubsub(event, context):
    dates = base64.b64decode(event['data']).decode('utf-8').split()
    for date in dates:
        if date == "TODAY":
            date = datetime.now().strftime("%m-%d-%Y")
        else:
            try:
                datetime.strptime(date, '%m-%d-%Y')
            except ValueError:
                sys.exit("Invalid date format: " + date)
        full_url = JHU_URL + date + ".csv"
        for success, info in streaming_bulk(client=ES, actions=generate_actions(full_url)):
            if not success:
                print('A document failed:', info)


def generate_actions(url):
    covid_df = pandas.read_csv(url, na_values=" ")
    covid_df = clean_data(covid_df)
    for row in covid_df.to_dict(orient="records"):
        key = (row["combined_key"] + os.path.basename(url)).encode()
        yield {
               "_index": "covid-19",
               "_op_type": "index",
               "_id": hashlib.sha1(key).hexdigest(),
               "_source": json.dumps(row, ignore_nan=True)
              }


def clean_data(df):
    df.rename(columns=str.lower, inplace=True)
    df.rename(columns=str.strip, inplace=True)
    df.rename(columns={"long_": "long"}, inplace=True)
    df.rename(columns={"admin2": "county"}, inplace=True)
    df.dropna(subset=['lat', 'long'], inplace=True)
    df['location'] = df["lat"].astype(str) + "," + df["long"].astype(str)
    df['last_update'] = df['last_update'] + "GMT"
    df['country_alpha3'] = df.apply(lambda row: gen_country_codes(row), axis=1)
    df.drop(columns=["lat", "long"], inplace=True)
    df.insert(0, "timestamp", datetime.now().strftime("%Y-%m-%d %H:%M:%S")+"EST")
    return df


def gen_country_codes(row):
    country = row['country_region']
    if country == "United Kingdom":
        country = "United Kingdom of Great Britain and Northern Ireland"
    elif country == "Bolivia":
        country = "Bolivia, Plurinational State of"
    elif country == "Brunei":
        country = "Brunei Darussalam"
    elif country == "Congo (Brazzaville)" or country == "Congo (Kinshasa)":
        country = "Congo, Democratic Republic of the"
    elif country == "Burma":
        country = "Myanmar"
    elif country == "Cote d'Ivoire":
        country = "Côte d'Ivoire"
    elif country == "Iran":
        country = "Iran, Islamic Republic of"
    elif country == "Korea, South":
        country = "Korea, Republic of"
    elif country == "Laos":
        country = "Lao People's Democratic Republic"
    elif country == "Moldova":
        country = "Moldova, Republic of"
    elif country == "Russia":
        country = "Russian Federation"
    elif country == "Syria":
        country = "Syrian Arab Republic"
    elif country == "Taiwan*":
        country = "Taiwan, Province of China"
    elif country == "Tanzania":
        country = "Tanzania, United Republic of"
    elif country == "Venezuela":
        country = "Venezuela, Bolivarian Republic of"
    elif country == "Vietnam":
        country = "Viet Nam"
    elif country == "West Bank and Gaza":
        country = "Palestine"
    return countries.get(country).alpha3


if __name__ == "__main__":
    e = {'data': base64.b64encode(b'TODAY')}
    hello_pubsub(e, "b")
