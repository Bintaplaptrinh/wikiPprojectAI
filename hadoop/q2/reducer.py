#!/usr/bin/env python3
import sys
import json
import heapq

def main():
    top_n = 10
    min_heap = []
    
    for line in sys.stdin:
        try:
            parts = line.strip().split('\t', 1)
            if len(parts) != 2:
                continue
            density, record_json = parts
            density = float(density)
            record = json.loads(record_json)
            
            if len(min_heap) < top_n:
                heapq.heappush(min_heap, (density, record))
            else:
                if density > min_heap[0][0]:
                    heapq.heappop(min_heap)
                    heapq.heappush(min_heap, (density, record))
        except:
            continue
    
    results = sorted(min_heap, key=lambda x: x[0], reverse=True)
    for density, record in results:
        print(json.dumps(record, ensure_ascii=False))

if __name__ == '__main__':
    main()
