#!/usr/bin/env python3
# ETL Mapper: Clean data, add pop_bucket, filter invalid records
# Input: JSONL from MongoDB (stdin)
# Output: country \t cleaned_json_record

import sys
import json
import re

def clean_number(value):
    if value is None or value == '' or str(value).upper() == 'N/A':
        return None
    if isinstance(value, (int, float)):
        return value
    s = str(value).replace(',', '').strip()
    try:
        return float(s) if '.' in s else int(s)
    except:
        return None

def get_pop_bucket(pop):
    if pop < 1_000_000:
        return '<1M'
    elif pop < 10_000_000:
        return '1M-10M'
    elif pop < 50_000_000:
        return '10M-50M'
    elif pop < 100_000_000:
        return '50M-100M'
    else:
        return '>100M'

def main():
    for line in sys.stdin:
        try:
            record = json.loads(line.strip())
            
            #extract fields
            country = record.get('Country', '').strip()
            region = record.get('Region', '').strip().title()
            subregion = record.get('Subregion', '').strip().title()
            
            #clean nums
            population = clean_number(record.get('population'))
            area_km2 = clean_number(record.get('area_km2'))
            density = clean_number(record.get('density'))
            
            #filter invalid records
            if not country:
                continue
            if not population or population <= 0:
                continue
            if not area_km2 or area_km2 <= 0:
                continue
            if not density or density <= 0:
                continue
            
            #add pop_bucket
            pop_bucket = get_pop_bucket(population)
            
            # rebuild clean record
            clean_record = {
                'Country': country,
                'Region': region,
                'Subregion': subregion,
                'population': int(population),
                'area_km2': float(area_km2),
                'density': float(density),
                'pop_bucket': pop_bucket
            }
            
            #output: key=country, value=json
            print(f"{country}\t{json.dumps(clean_record, ensure_ascii=False)}")
            
        except Exception as e:
            continue

if __name__ == '__main__':
    main()