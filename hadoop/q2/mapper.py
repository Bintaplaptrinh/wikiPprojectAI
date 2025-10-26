#!/usr/bin/env python3
# Q2 Mapper: Top 10 mật độ cao nhất
# Input: JSONL từ HDFS pop_clean.jsonl
# Output: density \t json_record
# Filter: area_km2 > 0

import sys
import json

def main():
    for line in sys.stdin:
        try:
            record = json.loads(line.strip())
            area_km2 = record.get('area_km2', 0)
            density = record.get('density', 0)
            
            if area_km2 and area_km2 > 0 and density:
                print(f"{density}\t{json.dumps(record, ensure_ascii=False)}")
        except:
            continue

if __name__ == '__main__':
    main()
