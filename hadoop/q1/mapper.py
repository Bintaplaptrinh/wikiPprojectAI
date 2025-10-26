#!/usr/bin/env python3
# Q1 Mapper: Tổng dân số theo Region
# Input: JSONL từ HDFS pop_clean.jsonl
# Output: region \t population

import sys
import json

def main():
    for line in sys.stdin:
        try:
            record = json.loads(line.strip())
            region = record.get('Region', '').strip()
            population = record.get('population', 0)
            
            if region and population:
                print(f"{region}\t{population}")
        except:
            continue

if __name__ == '__main__':
    main()
