import time
from scrape import scrape_poplulation_data, scrape_area_data, join_table
from storage import save_population_raw, save_area_raw, save_joined_table

def main():



    #scrape population
    print("\nscraping population data")
    df_pop, table_html = scrape_poplulation_data()
    if df_pop is None:
        print("error: failed to scrape population data")
        return
    print(f"scraped {len(df_pop)} population records")
    
    #scrape area
    print("\nscraping area/region data...")
    df_area, raw_text = scrape_area_data()
    if df_area is None:
        print("error: failed to scrape area data")
        return
    print(f"scraped {len(df_area)} area records")
    
    #join tables
    print("\njoining tables...")
    df_joined = join_table(df_pop, df_area)
    if df_joined is None:
        print("error: failed to join tables")
        return
    print(f"joined {len(df_joined)} records")
    
    #save to MongoDB (3 collections)
    print("\nsaving scraped data to MongoDB...")
    save_population_raw(df_pop, table_html)
    save_area_raw(df_area, raw_text)
    save_joined_table(df_joined)


if __name__ == "__main__":
    main()
    