#!/usr/bin/env python3
# ETL Reducer: Deduplicate and output clean JSONL
# Input: country \t json_record (sorted by country)
# Output: Pure JSONL (no key)

import sys
import json

def main():
    seen_countries = set()
    
    for line in sys.stdin:
        try:
            parts = line.strip().split('\t', 1)
            if len(parts) != 2:
                continue
                
            country, json_str = parts
            
            # deduplicate: keep first occurrence
            if country in seen_countries:
                continue
            seen_countries.add(country)
            
            # parse and output pure JSONL
            record = json.loads(json_str)
            print(json.dumps(record, ensure_ascii=False))
            
        except Exception as e:
            continue

if __name__ == '__main__':
    main()
