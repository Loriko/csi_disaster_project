import csv
import psycopg2
import psycopg2.extras
import sys
import json
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


def create_summary_dimension():
    # Create empty summary table
    create_summary_dimension_query = """
        DROP TABLE IF EXISTS fact;
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.summary_dimension;
        
        CREATE TABLE disaster_db.disaster_db_schema.summary_dimension
        (
          summary_key   SERIAL,
          summary       VARCHAR(300),
          keyword_1     VARCHAR(20),
          keyword_2     VARCHAR(20),
          keyword_3     VARCHAR(20),
          PRIMARY KEY (summary_key)
        );
    """
    execute_query(create_summary_dimension_query)


def populate_summary_dimension_row(csv_row):
    comment = csv_row[COMMENT_INDEX]
    if comment == "" or comment is None:
        comment = "NULL"
    else:
        comment = "'" + comment + "'"
    matching_keywords_list = []
    keyword_list = json.load(open("summary_keyword_list.json"))
    for keyword in keyword_list:
        if keyword in comment:
            matching_keywords_list.append(keyword)
    keyword1 = "NULL"
    keyword2 = "NULL"
    keyword3 = "NULL"
    if len(matching_keywords_list) >= 1:
        keyword1 = "'" + matching_keywords_list[0] + "'"
        if len(matching_keywords_list) >= 2:
            keyword2 = "'" + matching_keywords_list[1] + "'"
            if len(matching_keywords_list) >= 3:
                keyword3 = "'" + matching_keywords_list[2] + "'"
    sql_script = """
        INSERT INTO disaster_db.disaster_db_schema.summary_dimension(summary, keyword_1, keyword_2, keyword_3)
        VALUES (
          %s, %s, %s, %s
        );
    """ % (comment, keyword1, keyword2, keyword3)


def populate_disaster_dimension(csv_row):
    disaster_type = csv_row[EVENT_TYPE_INDEX]
    disaster_subgroup = csv_row[EVENT_SUBGROUP_INDEX]
    disaster_group = csv_row[EVENT_GROUP_INDEX]
    disaster_category = csv_row[EVENT_CATEGORY_INDEX]
    magnitude = csv_row[MAGNITUDE_INDEX]
    utility_people_affected = csv_row[UTILITY_PEOPLE_AFFECTED_INDEX]

    if magnitude is None:
        magnitude = 0
    
    if utility_people_affected is None:
        utility_people_affected = 0

    sql_script = """
        INSERT INTO disaster_db.disaster_db_schema.disaster_dimension(disaster_type, disaster_subgroup, disaster_group, disaster_category, magnitude, utility_people_affected)
        VALUES (
          %s, %s, %s, %s, %i, %i
        );
    """ % (disaster_type, disaster_subgroup, disaster_group, disaster_category, magnitude, utility_people_affected)

    execute_query(sql_script)

def create_disaster_dimension():
    # Create empty disaster table
    create_summary_dimension_query = """
        DROP TABLE IF EXISTS fact;
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.disaster_dimension;
        
        CREATE TABLE disaster_db.disaster_db_schema.disaster_dimension
        (
          disaster_key              SERIAL,
          disaster_type             VARCHAR(30),
          disaster_subgroup         VARCHAR(30),
          disaster_group            VARCHAR(30),
          disaster_category         VARCHAR(30),
          magnitude                 DECIMAL(18, 1),
          utility_people_affected   INT,
          PRIMARY KEY (disaster_key)
        );
    """
    execute_query(create_summary_dimension_query)


def populate_cost_dimension(csv_row):
    estimated_total_cost = csv_row[ESTIMATED_TOTAL_COST_INDEX]
    normalized_total_cost = csv_row[NORMALIZED_TOTAL_COST_INDEX]
    federal_payments = csv_row[FEDERAL_DFAA_PAYMENTS_INDEX]
    provincial_dfaa_payments = csv_row[PROVINCIAL_DFAA_PAYMENTS]
    provincial_department_payments = csv_row[PROVINCIAL_DEPARTMENT_PAYMENTS_INDEX]
    municipal_cost = csv_row[MUNICIPAL_COSTS_INDEX]
    ogd_cost = csv_row[OGD_COSTS_INDEX]
    insurance_payments = csv_row[INSURANCE_PAYMENTS_INDEX]
    ngo_cost = csv_row[NGO_PAYMENTS_INDEX]

    if estimated_total_cost is None:
        estimated_total_cost = 0

    if normalized_total_cost is None:
        normalized_total_cost = 0

    if federal_payments is None:
        federal_payments = 0

    if provincial_dfaa_payments is None:
        provincial_dfaa_payments = 0

    if provincial_department_payments is None:
        provincial_department_payments = 0

    if municipal_cost is None:
        municipal_cost = 0

    if ogd_cost is None:
        ogd_cost = 0

    if insurance_payments is None:
        insurance_payments = 0

    if ngo_cost is None:
        ngo_cost = 0

    sql_script = """
        INSERT INTO disaster_db.disaster_db_schema.cost_dimension(estimated_total_cost, normalized_total_cost, federal_dfaa_payments, 
            provincial_dfaa_payments, provincial_department_payments, municipal_cost, ogd_cost, insurance_payments, ngo_cost)
        VALUES (
          %i, %i, %i, %i, %i, %i, %i, %i, %i
        );
    """ % (estimated_total_cost, normalized_total_cost, federal_payments, provincial_dfaa_payments, provincial_department_payments,
            municipal_cost, ogd_cost, insurance_payments, ngo_cost)


def create_cost_dimension():
    # Create empty cost table
    create_cost_dimension_query = """
        DROP TABLE IF EXISTS fact;
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.cost_dimension;
        
        CREATE TABLE disaster_db.disaster_db_schema.cost_dimension
        (
          cost_key                          SERIAL,
          estimated_total_cost              INT,
          normalized_total_cost             INT,
          federal_dfaa_payments             INT,
          provincial_dfaa_payments          INT,
          provincial_department_payments    INT,
          municipal_cost                    INT,
          ogd_cost                          INT,
          insurance_payments                INT,
          ngo_cost                          INT,
          PRIMARY KEY (cost_key)
        );
    """
    execute_query(create_cost_dimension_query)


def create_data_mart():
    log("Starting creation of data mart")
    # Start calling create_populate methods here
    create_populate_date_dimension()
    create_summary_dimension()
    create_disaster_dimension()
    create_cost_dimension()
