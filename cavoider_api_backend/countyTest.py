import cavoider_api_backend.calculations as calculations
import cavoider_api_backend.APIRequests as api

def main():
    df_NYT_current = api.get_nyt_current_data()
    df_county_pop = api.get_current_county_data()
    df_NYT_historical = api.get_nyt_historical_data()
    df_master = calculations.main()

    # find the counties which were dropped in calculations
    all_fips = df_NYT_historical.drop_duplicates(subset=["fips"])
    all_fips = all_fips.dropna(subset=["fips"])
    df_dropped_county = all_fips.merge(df_master, how="left", on="fips", indicator=True)
    df_dropped_county = df_dropped_county[df_dropped_county["_merge"] == "left_only"]

    # find the counties which are not in the NYT dataset
    df_county_fips = df_county_pop.merge(all_fips, how="left", left_on="countyFIPS", right_on="fips", indicator=True)
    df_missing_county = df_county_fips[df_county_fips["_merge"] == "left_only"]
    # drops any lines without a fips code
    df_NYT_missing_county = df_missing_county[df_missing_county["countyFIPS"] != 0.0]
    print(df_dropped_county)
    print(df_NYT_missing_county)

if __name__ == "__main__":
    main()