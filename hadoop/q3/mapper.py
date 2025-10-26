#!/usr/bin/env python3
# Q3 Mapper: Phân phối pop_bucket
# Input: JSONL từ HDFS pop_clean.jsonl
# Output: pop_bucket \t 1

import sys
import json

def main():
    for line in sys.stdin:
        try:
            record = json.loads(line.strip())
            pop_bucket = record.get('pop_bucket', '')
            
            if pop_bucket:
                print(f"{pop_bucket}\t1")
        except:
            continue

if __name__ == '__main__':
    main()
        