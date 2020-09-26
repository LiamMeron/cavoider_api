import cavoider_api_backend.APIRequests as api
from datetime import datetime, timedelta

# get dataset
df_NYT_current = api.get_nyt_current_data()
df_NYT_previous = api.get_nyt_historical_data()
df_county_pop = api.get_current_county_data()

# create master dataframe
df_master = df_NYT_current.merge(df_county_pop, left_on="fips", right_on="countyFIPS")


# Calculate cases per 100000 people
def create_cases_by_population(df_master):
    df_master["cases/pop"] = (df_master["cases"] / df_master["population"]) * 100000


# Calculate deaths per 100000 people
def create_deaths_by_population(df_master):
    df_master["deaths/pop"] = (df_master["deaths"] / df_master["population"]) * 100000


# Calculate case fatality per 100000 people
def create_case_fatality_rate(df_master):
    df_master["deaths/cases"] = (df_master["deaths"] / df_master["cases"]) * 100


# Calculate number of new cases per day
# (uses yesterday and the day before that to find increase due to data reporting times)
def create_daily_case_count(df_master):
    current_date = df_NYT_current.iloc[0, 0]
    date = datetime.fromisoformat(current_date)
    yesterday = date - timedelta(days=1)
    day_and_time = yesterday.__str__()
    day_and_time = day_and_time.split(" ")
    yesterday = day_and_time[0]

    # calculate difference
    prev_day = date - timedelta(days=2)
    day_and_time = prev_day.__str__()
    day_and_time = day_and_time.split(" ")
    prev_day = day_and_time[0]
    df_yesterday = df_NYT_previous[df_NYT_previous["date"] == yesterday]
    df_yesterday = df_yesterday[["fips", "cases"]]
    df_prev_day = df_NYT_previous[df_NYT_previous["date"] == prev_day]
    df_prev_day = df_prev_day[["fips", "cases"]]
    df_prev_day = df_prev_day.rename(columns={"fips": "fips", "cases": "prev_cases"})
    df_daily_cases = df_yesterday.merge(df_prev_day, on="fips")
    df_master["new_daily_cases"] = (
        df_daily_cases["cases"] - df_daily_cases["prev_cases"]
    )


# Calculate number of new deaths per day
# (uses yesterday and the day before that to find increase due to data reporting times)
def create_daily_death_count(df_master):
    current_date = df_NYT_current.iloc[0, 0]
    date = datetime.fromisoformat(current_date)
    yesterday = date - timedelta(days=1)
    day_and_time = yesterday.__str__()
    day_and_time = day_and_time.split(" ")
    yesterday = day_and_time[0]

    # calculate difference
    prev_day = date - timedelta(days=2)
    day_and_time = prev_day.__str__()
    day_and_time = day_and_time.split(" ")
    prev_day = day_and_time[0]
    df_yesterday = df_NYT_previous[df_NYT_previous["date"] == yesterday]
    df_yesterday = df_yesterday[["fips", "deaths"]]
    df_prev_day = df_NYT_previous[df_NYT_previous["date"] == prev_day]
    df_prev_day = df_prev_day[["fips", "deaths"]]
    df_prev_day = df_prev_day.rename(columns={"fips": "fips", "deaths": "prev_deaths"})
    df_daily_deaths = df_yesterday.merge(df_prev_day, on="fips")
    df_master["new_daily_deaths"] = (
        df_daily_deaths["deaths"] - df_daily_deaths["prev_deaths"]
    )


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
    df_7_day_prev = df_7_day_prev.rename(
        columns={"fips": "fips", "cases": "prev_cases"}
    )
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
    df_14_day_prev = df_14_day_prev.rename(
        columns={"fips": "fips", "cases": "prev_cases"}
    )
    df_prev_week_cases = df_7_day_prev.merge(df_14_day_prev, on="fips")
    df_prev_week_change = df_prev_week_cases["cases"] - df_prev_week_cases["prev_cases"]

    # find the percent increase
    difference = df_week_change - df_prev_week_change
    df_master["percent_increase"] = (difference / df_prev_week_change) * 100


# Calculate active cases: number of new cases - new deaths within 30 days
# (see COVID Tracking Project - The Atlantic for more info)
def create_active_cases_estimate(df_master):
    current_date = df_NYT_current.iloc[0, 0]
    date = datetime.fromisoformat(current_date)

    # estimate active cases as the number of cases
    prev_30_days = date - timedelta(days=30)
    day_and_time = prev_30_days.__str__()
    day_and_time = day_and_time.split(" ")
    prev_30_days = day_and_time[0]
    df_30_day_prev = df_NYT_previous[df_NYT_previous["date"] == prev_30_days]
    df_30_day_prev = df_30_day_prev[["fips", "cases", "deaths"]]
    df_30_day_prev = df_30_day_prev.rename(
        columns={"fips": "fips", "cases": "prev_cases", "deaths": "prev_deaths"}
    )
    df_30_day_change = df_NYT_current.merge(df_30_day_prev, on="fips")
    df_new_cases = df_30_day_change["cases"] - df_30_day_change["prev_cases"]
    df_new_deaths = df_30_day_change["deaths"] - df_30_day_change["prev_deaths"]
    df_master["active_cases"] = df_new_cases - df_new_deaths


if __name__ == "__main__":
    #run all methods and save them to a new data set which contains all new calculations
    create_cases_by_population(df_master)
    create_deaths_by_population(df_master)
    create_case_fatality_rate(df_master)
    create_14_day_trend(df_master)
    create_active_cases_estimate(df_master)
    create_daily_case_count(df_master)
    create_daily_death_count(df_master)
    df_calculations = df_master
    print(df_calculations)
