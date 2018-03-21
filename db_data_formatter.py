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
NORTH_AMERICAN_HOLIDAYS = holidays.UnitedStates() + holidays.Canada() + holidays.Mexico()
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


def execute_scripts_from_file(filename):
    # Open and read the file as a single buffer
    fd = open(filename, "r")
    sqlFile = fd.read()
    fd.close()
    # all SQL commands (split on ';')
    sqlCommands = sqlFile.split(";")
    # Execute every command from the input file
    for command in sqlCommands:
        # This will skip and report errors
        # For example, if the tables do not yet exist, this will skip over
        # the DROP TABLE commands
        try:
            execute_query(command)
        except:
            print_stack_trace()


#
def populate_date_dimension_holidays(holidays_list):
    # gets all rows in the reported date dimension
    get_date_dimension_rows_query = """
        SELECT  date_key,
                date_actual
        FROM    public.date_dimension;
    """
    holiday_dates_list = []
    results = execute_query(get_date_dimension_rows_query)
    for row in results:
        date_dimension_id = row[0]
        is_holiday = row[1] in holidays_list
        holiday_text = holidays_list.get(row[1])
        if is_holiday:
            holiday_text = holiday_text.replace("'", "''")
            holiday_dates_list.append(date_dimension_id)
            update_query = """
                UPDATE  public.date_dimension
                SET     is_holiday = TRUE,
                        holiday_text = '%s'
                WHERE   posted_date_key = '%s';
            """ % (holiday_text, date_dimension_id)
            execute_query(update_query)
    print_success('Updated %d dates out of %d' % (len(holiday_dates_list), len(results)))


def create_populate_date_dimension():
    # Execute create_date_dimension_script
    execute_scripts_from_file("sql_scripts/create_date_dimension.sql")
    # Fill holidays using canadian holiday data
    populate_date_dimension_holidays(NORTH_AMERICAN_HOLIDAYS)


def create_data_mart():
    log("Starting creation of data mart")
    # Start calling create_populate methods here
    create_populate_date_dimension()
