import cavoider_api_backend.APIRequests as api

# get dataset
df_NYT_current = api.get_nyt_current_data()
df_NYT_previous = api.get_nyt_historical_data()
df_county_pop = api.get_current_county_data()

# Create master database
df_master = df_NYT_current.merge(df_county_pop, left_on="fips", right_on="countyFIPS")

# Calculate cases per population
df_master["cases/pop"] = (df_master["cases"]/df_master["population"])*100000

# Calculate 14 day trend
date = df_NYT_current.iloc[0, 0]
year = date.split("-")[0]
month = date.split("-")[1]
day = date.split("-")[2]
new_day = int(day) - 14
if new_day < 0:
    if month == '03':
        new_day = 28 + new_day
    elif int(month) % 2 == 0:
        new_day = 30 + new_day
    elif int(month) % 2 != 0:
        new_day = 31 + new_day
    if month == '01':
        month = '12'
        year = int(year) - 1
    else:
        month = '0' + str(int(month) - 1)
        if len(month) == 3:
            month = month[1:]
prev_date = str(year) + '-' + month + '-' + str(new_day)
print(prev_date)
df_prev_cases = df_NYT_previous[df_NYT_previous["date"] == prev_date]
df_14_day_prev = df_prev_cases[["fips", "cases"]]
df_14_day_prev = df_14_day_prev.rename(columns={"fips": "fips", "cases": "prev_cases"})
df_cases = df_NYT_current.merge(df_14_day_prev, on="fips")
df_master["14 day trend"] = (df_cases["cases"] - df_cases["prev_cases"])
print(df_master.head())