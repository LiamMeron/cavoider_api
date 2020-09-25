# TODO Download data from CDC API


# TODO Decide on data format for calculations in API


# TODO Create method to get daily increase in deaths and cases

# TODO Calculate Population Density

# TODO Calculate 14-day trend -> Place into buckets (increasing/flat/decreasing)

# TODO Calculate Confidence Score Factor

import logging

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)s.%(funcName)s +%(lineno)s: %(levelname)-8s [%(process)d] %(message)s",
)
