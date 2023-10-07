import json
import requests
import urllib.parse
import pandas as pd
from datetime import datetime, date
from config import tabelas

def ingestion(info, page):

    params = urllib.parse.urlencode({'format': 'json', 'page': page})
    response = requests.get(info["endpoint"] + params).json()

    if str(response) != "{'detail': 'Not found'}":
        data = response['results']
        
        path = info["path"]\
            .replace('$id',str(page))\
            .replace('$date',str(date.today()))
        
        with open(path, "w") as final:
            json.dump(data, final)

    return True

if __name__ == '__main__':
    for j in range(1,10):
        for key in tabelas:
            ingest_data = ingestion(tabelas[key], j)
