#!/usr/bin/env python3
import sys

def main():
    current_bucket = None
    total_count = 0
    
    for line in sys.stdin:
        try:
            parts = line.strip().split('\t')
            if len(parts) != 2:
                continue
            bucket, count = parts
            count = int(count)
            
            if current_bucket == bucket:
                total_count += count
            else:
                if current_bucket:
                    print(f"{current_bucket}\t{total_count}")
                current_bucket = bucket
                total_count = count
        except:
            continue
    
    if current_bucket:
        print(f"{current_bucket}\t{total_count}")

if __name__ == '__main__':
    main()