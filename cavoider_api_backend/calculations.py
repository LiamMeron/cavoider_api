import json
import cavoider_api_backend.APIRequests as api
from datetime import datetime, timedelta
from pandas import DataFrame
import pandas
import numpy


#from cavoider_api_backend.repository import Partition, AzureTableRepository


# Calculate covid data per 100000 people
def create_covid_data_by_population(covid_data: DataFrame, population: DataFrame, column_name: str):
    df_covid_data_and_pop = covid_data.merge(population, left_on="fips", right_on="countyFIPS")
    df_covid_data_and_pop[f"{column_name}_per_100k_people"] = (
        df_covid_data_and_pop[f"{column_name}"] / df_covid_data_and_pop["population"]) * 100000
    return df_covid_data_and_pop[["fips", f"{column_name}_per_100k_people"]]


# Calculate deaths per 100000 people
def create_deaths_per_100k_people(current_data: DataFrame, population: DataFrame):
    return create_covid_data_by_population(current_data[["fips", "deaths"]],
                                           population[["countyFIPS", "population"]], "deaths")


# Calculate cases per 100000 people
def create_cases_per_100k_people(current_data: DataFrame, population: DataFrame):
    return create_covid_data_by_population(current_data[["fips", "cases"]],
                                           population[["countyFIPS", "population"]], "cases")


# Calculate case fatality per 100000 people
def create_case_fatality_rate(current_data: DataFrame):
    case_fatality = current_data["deaths"] / current_data["cases"]
    fips_and_covid_data = {"fips": current_data["fips"], "case_fatality": case_fatality}
    df_case_fatality = pandas.DataFrame(fips_and_covid_data)
    return df_case_fatality


# Calculate difference between two different days
def create_daily_dif_between_columns(column_name: str, historical_data: DataFrame, current_data: DataFrame):
    today = current_data.iloc[0, 0]
    today = datetime.fromisoformat(today)
    yesterday = str(today - timedelta(days=1)).split(" ")
    yesterday = yesterday[0]
    two_days_ago = str(today - timedelta(days=2)).split(" ")
    two_days_ago = two_days_ago[0]

    # calculate difference
    df_yesterday = historical_data[historical_data["date"] == yesterday]
    df_yesterday = df_yesterday[["fips", f"{column_name}"]]
    df_two_days_ago = historical_data[historical_data["date"] == two_days_ago]
    df_two_days_ago = df_two_days_ago[["fips", f"{column_name}"]]
    df_two_days_ago = df_two_days_ago.rename(columns={f"{column_name}": f"prev_{column_name}"})

    df_daily_difference = df_yesterday.merge(df_two_days_ago, on="fips")

    df_daily_difference[f"new_daily_{column_name}"] = (
        df_daily_difference[f"{column_name}"] - df_daily_difference[f"prev_{column_name}"]
    )
    return df_daily_difference[["fips", f"new_daily_{column_name}"]]


# Calculate number of new cases per day
# (uses yesterday and the day before that to find increase due to delay reporting times)
def create_daily_case_count(historical_data: DataFrame, current_data: DataFrame):
    return create_daily_dif_between_columns("cases", historical_data, current_data)


# Calculate number of new deaths per day
# (uses yesterday and the day before that to find increase due to delay reporting times)
def create_daily_death_count(historical_data: DataFrame, current_data: DataFrame):
    return create_daily_dif_between_columns("deaths", historical_data, current_data)


# Calculate difference between week
def create_diff_between_columns(column_name: str, historical_data: DataFrame,
                                       current_data: DataFrame, num_days_elapsed: int):
    today = current_data.iloc[0, 0]
    today = datetime.fromisoformat(today)
    elapsed_days = str(today - timedelta(days=num_days_elapsed)).split(" ")
    elapsed_days = elapsed_days[0]

    # calculate difference
    df_today = current_data[["fips", f"{column_name}"]]
    df_elapsed = historical_data[historical_data["date"] == elapsed_days]
    df_elapsed = df_elapsed[["fips", f"{column_name}"]]
    df_elapsed = df_elapsed.rename(columns={f"{column_name}": f"prev_{column_name}"})

    df_difference = df_today.merge(df_elapsed, on="fips")

    df_difference[f"new_difference_{column_name}"] = (
            df_difference[f"{column_name}"] - df_difference[f"prev_{column_name}"]
    )
    return df_difference[["fips", f"new_difference_{column_name}"]]


# Calculate 14 day trend
def create_14_day_case_trend(historical_data: DataFrame, current_data: DataFrame):
    # calculate week one
    df_week_2 = create_diff_between_columns("cases", historical_data, current_data, 7)
    df_week_2 = df_week_2.rename(columns={"new_difference_cases": "week 2"})

    # calculate week two
    df_14_days = create_diff_between_columns("cases", historical_data, current_data, 14)
    df_14_days = df_14_days.rename(columns={"new_difference_cases": "14 days"})

    # calculate the percent increase
    df_both_weeks = df_14_days.merge(df_week_2, on="fips")
    df_both_weeks["week 1"] = df_both_weeks["14 days"] - df_both_weeks["week 2"]
    df_both_weeks["percent_change_14_days"] = ((df_both_weeks["week 2"] - df_both_weeks["week 1"]) / df_both_weeks["week 1"].abs()) * 100

    # replace the any percent change which is equal to infinity
    df_both_weeks["percent_change_14_days"] = df_both_weeks["percent_change_14_days"].replace([numpy.inf], "na")

    return df_both_weeks[["fips", "percent_change_14_days"]]


# Calculate active cases: number of new cases - new deaths within 30 days
# (see COVID Tracking Project - The Atlantic for more info)
def create_active_cases_estimate(historical_data: DataFrame, current_data: DataFrame):
    # calculate change in cases and deaths in last 30 days
    df_30_day_cases = create_diff_between_columns("cases", historical_data, current_data, 30)
    df_30_day_deaths = create_diff_between_columns("deaths", historical_data, current_data, 30)
    df_30_day_change = df_30_day_cases.merge(df_30_day_deaths, on="fips")

    # calculate active cases
    df_30_day_change["active_cases_est"] = \
        df_30_day_change["new_difference_cases"] - df_30_day_change["new_difference_deaths"]

    return df_30_day_change[["fips", "active_cases_est"]]

# Modify a specified row and cell in the data table to account for reporting differences
def modify_datatable(current_data: DataFrame, column_name: str, fips: int, county_name: str, num_counties: int):
    selected_row = current_data[current_data["county"] == f"{county_name}"]
    selected_row_covid_data = int(selected_row[f"{column_name}"] / num_counties)
    row_to_change_covid_data = int(current_data[current_data["fips"] == fips][f"{column_name}"])
    new_data = selected_row_covid_data + row_to_change_covid_data
    current_data.loc[current_data.fips == fips, [f"{column_name}"]] = new_data


def main():
    # pull in data
    df_NYT_current = api.get_nyt_current_data()
    df_NYT_historical = api.get_nyt_historical_data()
    df_county_pop = api.get_current_county_data()

    # modify data
    # replace nyc with a preselected fips code since one is not given
    df_NYT_current.loc[df_NYT_current.county == "New York City", ["fips"]] = 112090

    # add the joplin data to the other counties
    # jasper county
    modify_datatable(df_NYT_current, "cases", 29097, "Joplin", 2)
    modify_datatable(df_NYT_current, "deaths", 29097, "Joplin", 2)
    modify_datatable(df_NYT_current, "confirmed_cases", 29097, "Joplin", 2)
    modify_datatable(df_NYT_current, "confirmed_deaths", 29097, "Joplin", 2)
    # newton county
    modify_datatable(df_NYT_current, "cases", 29145, "Joplin", 2)
    modify_datatable(df_NYT_current, "deaths", 29145, "Joplin", 2)
    modify_datatable(df_NYT_current, "confirmed_cases", 29145, "Joplin", 2)
    modify_datatable(df_NYT_current, "confirmed_deaths", 29145, "Joplin", 2)

    # add the kansas city data to the other counties
    # cass county
    modify_datatable(df_NYT_current, "cases", 29037, "Kansas City", 4)
    modify_datatable(df_NYT_current, "deaths", 29037, "Kansas City", 4)
    modify_datatable(df_NYT_current, "confirmed_cases", 29037, "Kansas City", 4)
    modify_datatable(df_NYT_current, "confirmed_deaths", 29037, "Kansas City", 4)
    # clay county
    modify_datatable(df_NYT_current, "cases", 29047, "Kansas City", 4)
    modify_datatable(df_NYT_current, "deaths", 29047, "Kansas City", 4)
    modify_datatable(df_NYT_current, "confirmed_cases", 29047, "Kansas City", 4)
    modify_datatable(df_NYT_current, "confirmed_deaths", 29047, "Kansas City", 4)
    # jackson county
    modify_datatable(df_NYT_current, "cases", 29095, "Kansas City", 4)
    modify_datatable(df_NYT_current, "deaths", 29095, "Kansas City", 4)
    modify_datatable(df_NYT_current, "confirmed_cases", 29095, "Kansas City", 4)
    modify_datatable(df_NYT_current, "confirmed_deaths", 29095, "Kansas City", 4)
    # platte county
    modify_datatable(df_NYT_current, "cases", 29165, "Kansas City", 4)
    modify_datatable(df_NYT_current, "deaths", 29165, "Kansas City", 4)
    modify_datatable(df_NYT_current, "confirmed_cases", 29165, "Kansas City", 4)
    modify_datatable(df_NYT_current, "confirmed_deaths", 29165, "Kansas City", 4)

    # Calculate all the statistics
    cases_per_100k_people = create_cases_per_100k_people(df_NYT_current, df_county_pop)
    deaths_per_100k_people = create_deaths_per_100k_people(df_NYT_current, df_county_pop)
    daily_case_change = create_daily_case_count(df_NYT_historical, df_NYT_current)
    daily_death_change = create_daily_death_count(df_NYT_historical, df_NYT_current)
    case_fatality_rate = create_case_fatality_rate(df_NYT_current)
    case_trend_14_days = create_14_day_case_trend(df_NYT_historical, df_NYT_current)
    active_cases_est = create_active_cases_estimate(df_NYT_historical, df_NYT_current)

    # Create a master data table with all relevant statistics
    df_current_data = df_NYT_current[["date", "county", "state", "fips", "cases", "deaths"]]
    df_master = df_current_data.merge(cases_per_100k_people, on="fips")
    df_master = df_master.merge(deaths_per_100k_people, on="fips")
    df_master = df_master.merge(daily_case_change, on="fips")
    df_master = df_master.merge(daily_death_change, on="fips")
    df_master = df_master.merge(case_fatality_rate, on="fips")
    df_master = df_master.merge(case_trend_14_days, on="fips")
    df_master = df_master.merge(active_cases_est, on="fips")

    # reformat data frame
    df_master = df_master.rename(columns={"date": "report_date"})
    df_master["fips"] = df_master["fips"].astype(int)

    # print all columns in data frame
    pandas.set_option("max_columns", None)
    print(df_master)

    #df_to_dict = json.loads(df_master.head(n=200).to_json(orient="table", index=False))[
    #    "data"
    #]

    #repo = AzureTableRepository("Test01")
    #for record in df_to_dict:
    #    repo.add(Partition.latest_county_report, record)


if __name__ == "__main__":
    main()
