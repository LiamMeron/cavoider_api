from environs import Env

env = Env()
# Reads the .env file into env_vars, if it exists
env.read_env()

CDC_EXCESS_DEATHS_ENDPOINT = env.str("CDC_EXCESS_DEATHS_ENDPOINT") #=> https://data.cdc.gov/resource/r8kw-7aab.json
NYT_HISTORICAL_COUNTIES_ENDPOINT = env.str("NYT_HISTORICAL_COUNTIES_ENDPOINT") #=> https://github.com/nytimes/covid-19-data/raw/master/us-counties.csv
NYT_CURRENT_COUNTIES_ENDPOINT = env.str("NYT_CURRENT_COUNTIES_ENDPOINT") #=>https://github.com/nytimes/covid-19-data/raw/master/live/us-counties.csv
CURRENT_COUNTY_POP_ENDPOINT = env.str("CURRENT_COUNTY_POP_ENDPOINT") #=>https://usafactsstatic.blob.core.windows.net/public/data/covid-19/covid_county_population_usafacts.csv

if __name__ == "__main__":
    print(CDC_EXCESS_DEATHS_ENDPOINT)
    print(NYT_CURRENT_COUNTIES_ENDPOINT)
    print(NYT_HISTORICAL_COUNTIES_ENDPOINT)