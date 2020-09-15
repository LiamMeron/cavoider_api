import cavoider_api_backend.APIRequests as api
from datetime import datetime, timedelta
import pandas

# get dataset
df_NYT_current = api.get_nyt_current_data()
df_NYT_previous = api.get_nyt_historical_data()
df_county_pop = api.get_current_county_data()

# create master dataframe
df_master = df_NYT_current.merge(df_county_pop, left_on="fips", right_on="countyFIPS")

# Calculate cases per 100000 people
def create_cases_by_population(df_master):
    df_master["cases/pop"] = (df_master["cases"]/df_master["population"])*100000

# Calculate deaths per 100000 people
def create_deaths_by_population(df_master):
    df_master["deaths/pop"] = (df_master["deaths"]/df_master["population"])*100000

# Calculate case fatality per 100000 people
def create_case_fatality_rate_by_population(df_master):
    df_master["deaths/cases"] = (df_master["deaths"]/df_master["cases"])*100000

# Calculate 14 day trend
def create_14_day_trend(df_master):
    current_date = df_NYT_current.iloc[0, 0]
    date = datetime.fromisoformat(current_date)

    # calculate week one
    prev_7_days = date - timedelta(days=7)
    day_and_time = prev_7_days.__str__()
    day_and_time = day_and_time.split(" ")
    prev_7_days = day_and_time[0]
    df_7_day_prev = df_NYT_previous[df_NYT_previous["date"] == prev_7_days]
    df_7_day_prev = df_7_day_prev[["fips", "cases"]]
    df_7_day_prev = df_7_day_prev.rename(columns={"fips": "fips", "cases":"prev_cases"})
    df_week_cases = df_NYT_current.merge(df_7_day_prev, on="fips")
    df_week_change = df_week_cases["cases"] - df_week_cases["prev_cases"]

    # calculate week two
    prev_14_days = date - timedelta(days=14)
    day_and_time = prev_14_days.__str__()
    day_and_time = day_and_time.split(" ")
    prev_14_days = day_and_time[0]
    df_14_day_prev = df_NYT_previous[df_NYT_previous["date"] == prev_14_days]
    df_7_day_prev = df_NYT_previous[df_NYT_previous["date"] == prev_7_days]
    df_14_day_prev = df_14_day_prev[["fips", "cases"]]
    df_14_day_prev = df_14_day_prev.rename(columns={"fips": "fips", "cases":"prev_cases"})
    df_prev_week_cases = df_7_day_prev.merge(df_14_day_prev, on="fips")
    df_prev_week_change = df_prev_week_cases["cases"] - df_prev_week_cases["prev_cases"]

    # find the percent increase
    difference = df_week_change - df_prev_week_change
    df_master["percent_increase"] = (difference/df_prev_week_change)*100


if __name__ == "__main__":
    create_14_day_trend(df_master)
    print(df_master.head())