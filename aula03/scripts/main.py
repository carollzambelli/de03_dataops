import json
import os
import requests
import urllib.parse
import pandas as pd
from config import configs_work, configs_dw
from utils import Saneamento, sw_work_to_dw

def ingestion(page):

    payload = {}

    for key in configs_work:
        params = urllib.parse.urlencode({'format': 'json', 'page': page})
        response = requests.get(configs_work[key]["endpoint"] + params).json()

        if str(response) != "{'detail': 'Not found'}":
            data = response['results']
            raw_path = configs_work[key]["raw_path"].replace('$id',str(page))
            with open(raw_path, "w") as final: json.dump(data, final)
            payload[key] = raw_path

    return payload


def preparation_work(payload):
    
    if len(payload) > 0:
        for key in payload:
            json_data = json.load(open(payload[key]))
            df = pd.DataFrame.from_records(json_data)
            san = Saneamento(df, configs_work, key)
            san.rename()
            san.normalize_dtype()
            san.normalize_null()
            san.tipagem()
            san.normalize_str()
            san.save_work()
    else:
        print("sem dados novos")

    return True

if __name__ == '__main__':
    for j in range(1,10):
        payload = ingestion(j)
        preparation_work(payload)
    sw_work_to_dw(configs_work, configs_dw)


