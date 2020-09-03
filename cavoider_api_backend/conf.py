from environs import Env

env = Env()
# Reads the .env file into env_vars, if it exists
env.read_env()

CDC_EXCESS_DEATHS_ENDPOINT = env.str("CDC_EXCESS_DEATHS_ENDPOINT") #=> https://data.cdc.gov/resource/r8kw-7aab.json

if __name__ == "__main__":
    print(CDC_EXCESS_DEATHS_ENDPOINT)