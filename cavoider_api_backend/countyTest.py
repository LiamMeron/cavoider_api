import cavoider_api_backend.calculations as calculations
import cavoider_api_backend.APIRequests as api

def main():
    df_NYT_current = api.get_nyt_current_data()
    df_county_pop = api.get_current_county_data()
    df_master = calculations.main()

    # if the file is only merged by the left, it is in the full county list, but not in final calculation data table
    df_county_fips = df_county_pop.merge(df_master, how="left", left_on="countyFIPS", right_on="fips", indicator=True)
    df_missing_county = df_county_fips[df_county_fips["_merge"] == "left_only"]
    # drops any lines without a fips code
    df_missing_county = df_missing_county[df_missing_county["countyFIPS"] != 0.0]
    # conducts another merge to find the counties which get dropped in calculations
    df_missing_county = df_missing_county.drop(columns=["_merge"])
    df_missing_county = df_missing_county.merge(df_NYT_current, how="left", left_on="countyFIPS", right_on="fips", indicator=True)
    df_dropped_county = df_missing_county[df_missing_county["_merge"] == "both"]
    # finds the counties which are missing from the NYT dataset but are in the full county list
    df_NYT_missing_county = df_missing_county[df_missing_county["_merge"] == "left_only"]
    print(df_dropped_county)
    print(df_NYT_missing_county)

if __name__ == "__main__":
    main()