import csv
import psycopg2
import psycopg2.extras
import sys
import pprint
from datetime import date
import holidays
import traceback

LOGGING_TURNED_ON = True
CONNECTION_STRING = "host='localhost' dbname='disaster_db' user='postgres' password='password'"
us_holidays = holidays.UnitedStates()
CSV_FILE_LOCATION = "canadian_disaster_database_source_data.csv"
CONNECTION = psycopg2.connect(CONNECTION_STRING)

# labeling the indexes of the columns of the source disaster csv file
EVENT_CATEGORY_INDEX = 0
EVENT_GROUP_INDEX = 1
EVENT_SUBGROUP_INDEX = 2
EVENT_TYPE_INDEX = 3
PLACE_INDEX = 4
EVENT_START_DATE_INDEX = 5
COMMENT_INDEX = 6
FATALITIES_INDEX = 7
INJURED_INFECTED_INDEX = 8
EVACUATED_INDEX = 9
ESTIMATED_TOTAL_COST_INDEX = 10
NORMALIZED_TOTAL_COST_INDEX = 11
EVENT_END_DATE_INDEX = 12
FEDERAL_DFAA_PAYMENTS_INDEX = 13
PROVINCIAL_DFAA_PAYMENTS = 14
PROVINCIAL_DEPARTMENT_PAYMENTS_INDEX = 15
MUNICIPAL_COSTS_INDEX = 16
OGD_COSTS_INDEX = 17
INSURANCE_PAYMENTS_INDEX = 18
NGO_PAYMENTS_INDEX = 19
UTILITY_PEOPLE_AFFECTED_INDEX = 20
MAGNITUDE_INDEX = 21


# enum containing the color codes for coloring the console output
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


class MissingDimensionValueException(Exception):
    pass


class DuplicateRowException(Exception):
    pass


def log(message):
    if LOGGING_TURNED_ON:
        print message


# Prints even if logging is turned off
def print_stack_trace():
    print bcolors.FAIL
    traceback.print_exc()
    print bcolors.ENDC


# Prints even if logging is turned off
def print_success(success_message):
    print bcolors.OKGREEN
    print success_message
    print bcolors.ENDC


def execute_query(query):
    # Configure cursor
    cursor = CONNECTION.cursor(cursor_factory=psycopg2.extras.DictCursor)
    results = []
    try:
        cursor.execute(query)
        if "SELECT" in cursor.statusmessage and "INTO" not in cursor.statusmessage:
            results = cursor.fetchall()
        log("Query successful")
    except:
        print_stack_trace()
    finally:
        cursor.close()
        CONNECTION.commit()
        CONNECTION.close()
        log('Connection closed')
        return results


def create_data_mart():
    log("Starting creation of data mart")
    # Start calling create_populate methods here
