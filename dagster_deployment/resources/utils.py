from dagster import ConfigurableResource
from typing import Union
import requests

class LocalIoResource(ConfigurableResource):
    def os_save_json(data:dict, filepath:str)->None:
        '''Save a dict as a json file'''
        os.makedirs(raw_fantasy_dir, exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)
        return

    def polars_to_parquet(filedir:str, filename:str, data:Union[pl.DataFrame, pl.LazyFrame])->None:
        '''Write a polars frame to parquet file'''
        os.makedirs(filedir, exist_ok=True)
        if isinstance(data, pl.DataFrame):
            data.write_parquet(f'{filedir}/{filename}.parquet')
        elif isinstance(data, pl.LazyFrame):
            #streaming not supported for all operations data.sink_parquet(f'{filedir}/{filename}.parquet')
            data.collect().write_parquet(f'{filedir}/{filename}.parquet')
        else:
            raise NotImplementedError('Data type not supported')
        return

class ApiResource(ConfigurableResource):
    def get_request(url:str, params:dict=None) -> dict:
        '''Make a get request and return a dict'''
        resp = requests.get(url, params)
        try:
            resp.raise_for_status()
        except requests.exceptions.HTTPError as e:
            if resp.status_code == 404:
                logging.warning(f'No data found for {url}')
                return {}
        resp_dict = resp.json()
        return resp_dict

