import cavoider_api_backend.APIRequests as api

# Create master database
df_NYT_Current = api.get_nyt_current_data()
df_county_pop = api.get_current_county_data()

# Calculate cases per population
df_master = df_NYT_Current.merge(df_county_pop, left_on="fips", right_on="countyFIPS")
df_master["cases/pop"] = (df_master["cases"]/df_master["population"])*100000
print(df_master.head())

# Calculate 14 day trend
