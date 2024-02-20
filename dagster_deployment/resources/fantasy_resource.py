from dagster import ConfigurableResource
from resources.utils import *
from assets import constants

class FantasyResource(ConfigurableResource):
    def save_raw_json(data:dict, filename:str, year:int=None):
        '''Save the raw data extracted from the Fantasy APIs as json files'''
        if data:
            if year:
                raw_fantasy_dir = f'data/landing/fantasy/{year}'
            else:
                raw_fantasy_dir = f'data/landing/fantasy'
            filepath = os.path.join(raw_fantasy_dir, f'{filename}.json')
            utils.LocalIoResource.os_save_json(data=data, filepath=filepath)
        else:
            logging.warning(f'Empty dict received for {year} - {filename}')
        return