import re
from unicodedata import name
from bs4 import BeautifulSoup
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from standardize_country import get_country_mapping, normalize_name


def scrape_poplulation_data():
#data type:
#location,density,population,area_km2

    url = "https://en.wikipedia.org/wiki/List_of_countries_and_dependencies_by_population_density"
    
    headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/91.0.4472.124 Safari/537.36"
        }    
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retry))
    try:
        print("starting scrape population data")
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        table_classes = ['wikitable sortable', 'wikitable', 'sortable wikitable', 'wikitable sortable mw-datatable']
        table = None
        
        for cls in table_classes:
            table = soup.find('table', {'class': cls})
            if table:
                break
            
        if not table:
            raise Exception("not found any table")
        
        table_raw_html = str(table)
        
        
        rows = table.find_all('tr')
        data = []
        for row in rows[1:]:
            cols  = row.find_all('td')
            if len(cols) >= 5:
                
                
                if 'scope' in cols[0].attrs and cols[0].attrs['scope'] == 'rowgroup':
                    continue

                location = cols[0].get_text(strip=True)
                density_km_str = cols[1].get_text(strip=True).replace(',', '')
                population_str = cols[3].get_text(strip=True).replace(',', '')
                area_km_str = cols[4].get_text(strip=True).replace(',', '')
                
                
                
                if location:
                    data.append({
                        'location': location,
                        'density': density_km_str,
                        'population': population_str,
                        'area_km2': area_km_str
                    })
                    
                    
        df = pd.DataFrame(data)
        
        df['density'] = pd.to_numeric(df['density'], errors='coerce')
        df['population'] = pd.to_numeric(df['population'], errors='coerce').astype('Int64')
        df['area_km2'] = pd.to_numeric(df['area_km2'], errors='coerce').astype(float)
        df = df.replace('', pd.NA)
        df = df[df['location'].notna() & (df['location'] != '')]
        
        return df, table_raw_html
    except requests.RequestException as e:
        print(f"error in requests step: {str(e)}")
        return None
    except Exception as e:
        print(f"error: {str(e)}")
        return None
    finally:
        session.close()
    
def scrape_area_data():
#data type:
#counttry,region,subregion
    url ="https://en.wikipedia.org/wiki/United_Nations_geoscheme"
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/91.0.4472.124 Safari/537.36"
    }

    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
    session.mount('https://', HTTPAdapter(max_retries=retry))
    try:
        print("starting scrape area data")
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.text, 'html.parser')
        
        content = soup.find("div", {"class": "mw-parser-output"})

        raw = content.get_text(strip=True)

        data = []
        region = None
        subregion = None
        
        for tag in content.find_all(["h2", "h3", "ul"]):
            if tag.name == "h2":
                region = tag.get_text(strip=True).replace("[edit]", "")
                subregion = None
            elif tag.name == "h3":
                subregion = tag.get_text(strip=True).replace("[edit]", "")
            elif tag.name == "ul":
                for li in tag.find_all("li", recursive=False):
                    a = li.find("a")
                    if not a:
                        continue
                    country = a.get_text(strip=True)
                    
                    if any(x in country.lower() for x in ["edit", "geoscheme", "references", "see also"]):
                        continue
                    if region and subregion and country:
                        data.append({
                            "Country": country,
                            "Region": region,
                            "Subregion": subregion
                        })
        df = pd.DataFrame(data)

        return df, raw
    
    except requests.RequestException as e:
        print(f"error in requests step: {str(e)}")
        return None
    except Exception as e:
        print(f"error: {str(e)}")
        return None
    finally:
        session.close()



def join_table(df_population, df_area):

#form of data:
# country, region, subregion, population, area_km2, density

    if df_population is None or df_area is None:
        print("one of dataframe is none")
        return None
    
    try:
        print("starting join table")
        
        #filter out
        df_population = df_population[~df_population['location'].isin(['World'])].copy()
        df_area = df_area[~df_area['Country'].isin(['World'])].copy()

        #standardize countries
        mapping_dict = get_country_mapping()
        df_population['std_location'] = df_population['location'].apply(lambda x: normalize_name(x, mapping_dict))
        df_area['std_Country'] = df_area['Country'].apply(lambda x: normalize_name(x, mapping_dict))
        
        df_merged = pd.merge(
            df_area,
            df_population,
            left_on='std_Country',
            right_on='std_location',
            how='inner'
        )
        
        #sort
        df = df_merged[
            [
                'Country',
                'Region',
                'Subregion',
                'population',
                'area_km2',
                'density'
            ]
        ].sort_values(by='Country').reset_index(drop=True)
        
        print(f'joined 2 tables with {len(df)} rows')
        return df        
    except Exception as e:
        print(f"error in join step: {str(e)}")
        return None
    
