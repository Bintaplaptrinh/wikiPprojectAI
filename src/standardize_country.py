import pandas as pd
import re

def get_country_mapping():

    mapping = {

        'south korea': 'korea, republic of',
        'korea, republic of': 'korea, republic of',
        'north korea': "korea, democratic people's republic of",
        "korea, democratic people's republic of": "korea, democratic people's republic of",
        'vietnam': 'viet nam',
        'viet nam': 'viet nam',
        'united kingdom': 'united kingdom of great britain and northern ireland',
        'united kingdom of great britain and northern ireland': 'united kingdom of great britain and northern ireland',
        'dr congo': 'democratic republic of the congo',
        'democratic republic of the congo': 'democratic republic of the congo',
        'congo': 'congo',
        'ivory coast': "côte d'ivoire",
        "côte d'ivoire": "côte d'ivoire",
        'czech republic': 'czechia',
        'czechia': 'czechia',
        'united states': 'united states',
        'vatican city': 'holy see',
        'holy see': 'holy see',
        'russia': 'russian federation',
        'russian federation': 'russian federation',
        'palestine': 'state of palestine',
        'state of palestine': 'state of palestine',
        'brunei': 'brunei darussalam',
        'brunei darussalam': 'brunei darussalam',
        'iran': 'iran (islamic republic of)',
        'iran (islamic republic of)': 'iran (islamic republic of)',
        'são tomé and príncipe': 'sao tome and principe',
        'sao tome and principe': 'sao tome and principe',
        'tanzania': 'united republic of tanzania',
        'united republic of tanzania': 'united republic of tanzania',
        'cape verde': 'cabo verde',
        'cabo verde': 'cabo verde',
        'eswatini': 'eswatini',
        'micronesia': 'micronesia (federated states of)',
        'micronesia (federated states of)': 'micronesia (federated states of)',
        'pitcairn islands': 'pitcairn',
        'pitcairn': 'pitcairn',
        'laos': "lao people's democratic republic",
        "lao people's democratic republic": "lao people's democratic republic",
        'moldova': 'republic of moldova',
        'republic of moldova': 'republic of moldova',
        'syria': 'syrian arab republic',
        'syrian arab republic': 'syrian arab republic',
        'hong kong': 'china, hong kong special administrative region',
        'macau': 'china, macao special administrative region',
        'u.s. virgin islands': 'united states virgin islands',
        'saint martin': 'saint martin (french part)',
        'wallis and futuna': 'wallis and futuna islands',
        'åland': 'åland islands'
    }
    return mapping

def normalize_name(name, mapping_dict):

    if not isinstance(name, str):
        return None

    clean_name = re.sub(r'\[.*?\]|\(.*?\)','', name).strip()
    
    lower_name = clean_name.lower()
    
    return mapping_dict.get(lower_name, lower_name)

