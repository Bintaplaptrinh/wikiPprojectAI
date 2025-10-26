#!/usr/bin/env python3
# Q1 Reducer: Cộng tổng dân số theo Region
# Input: region \t population (sorted by region)
# Output: region \t total_population

import sys

def main():
    current_region = None
    total_pop = 0
    
    for line in sys.stdin:
        try:
            parts = line.strip().split('\t')
            if len(parts) != 2:
                continue
                
            region, pop = parts
            pop = int(pop)
            
            if current_region == region:
                total_pop += pop
            else:
                if current_region:
                    print(f"{current_region}\t{total_pop}")
                current_region = region
                total_pop = pop
        except:
            continue
    
    if current_region:
        print(f"{current_region}\t{total_pop}")

if __name__ == '__main__':
    main()
        