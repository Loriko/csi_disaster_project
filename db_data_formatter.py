# coding=utf-8
import csv
import psycopg2
import psycopg2.extras
import sys
import json
import pprint
from datetime import date
import holidays
import traceback

LOGGING_TURNED_ON = False
CONNECTION_STRING = "host='localhost' dbname='disaster_db' user='postgres' password='password'"
NORTH_AMERICAN_HOLIDAYS = holidays.UnitedStates() + holidays.Canada() + holidays.Mexico()
CSV_FILE_LOCATION = "canadian_disaster_database_source_data.csv"
OLD_LOCATION_FILE_LOCATION = "place_column.csv"
LOCATION_FILE_LOCATION = "location_data.csv"
CITY_PROVINCE_FILE_LOCATION = "city_province_data.csv"
PROBLEMATIC_PLACES_FILE_LOCATION = "problematic_places.csv"
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

# Quebec is used as the biggest city for quebec even though it isn't the biggest city because sometimes
# only "Quebec" is specified for the place and there is ambiguity to whether it denotes the city or the province
MAIN_CITY_FOR_PROVINCES = {
    "QC": "quebec",
    "ON": "toronto",
    "NL": "st. johns",
    "PE": "charlottetown",
    "NS": "halifax",
    "NB": "moncton",
    "MB": "winnipeg",
    "SK": "saskatoon",
    "AB": "calgary",
    "BC": "vancouver",
    "YT": "whitehorse",
    "NT": "yellowknife",
    "NU": "iqaluit"
}
TO_PROVINCE_CODE_CONVERSION_MAP = {
    "newfoundland and labrador": "NL",
    "prince edward island": "PE",
    "nova scotia": "NS",
    "new brunswick": "NB",
    "quebec": "QC",
    "ontario": "ON",
    "manitoba": "MB",
    "saskatchewan": "SK",
    "alberta": "AB",
    "british columbia": "BC",
    "yukon": "YT",
    "northwest territories": "NT",
    "nunavut": "NU",
    " nl": "NL",
    " pe": "PE",
    " ns": "NS",
    " nb": "NB",
    " qc": "QC",
    " on": "ON",
    " mb": "MB",
    " sk": "SK",
    " ab": "AB",
    " bc": "BC",
    " yt": "YT",
    " nt": "NT",
    " nu": "NU"
}

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

    if magnitude == "":
        magnitude = "NULL"
    
    if utility_people_affected == "":
        utility_people_affected = "NULL"

    sql_script = """
        INSERT INTO disaster_db.disaster_db_schema.disaster_dimension(disaster_type, disaster_subgroup, disaster_group, disaster_category, magnitude, utility_people_affected)
        VALUES (
          %s, %s, %s, %s, %s, %s
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

    if estimated_total_cost == "":
        estimated_total_cost = "NULL"

    if normalized_total_cost == "":
        normalized_total_cost = "NULL"

    if federal_payments == "":
        federal_payments = "NULL"

    if provincial_dfaa_payments == "":
        provincial_dfaa_payments = "NULL"

    if provincial_department_payments == "":
        provincial_department_payments = "NULL"

    if municipal_cost == "":
        municipal_cost = "NULL"

    if ogd_cost == "":
        ogd_cost = "NULL"

    if insurance_payments == "":
        insurance_payments = "NULL"

    if ngo_cost == "":
        ngo_cost = "NULL"

    sql_script = """
        INSERT INTO disaster_db.disaster_db_schema.cost_dimension(estimated_total_cost, normalized_total_cost, federal_dfaa_payments, 
            provincial_dfaa_payments, provincial_department_payments, municipal_cost, ogd_cost, insurance_payments, ngo_cost)
        VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s
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


# Returns a map that maps every valid place string to its id in the database
# Only populates rows for canadian locations. Non canadian locations will have to be created some other way
def create_populate_location_dimension():
    # Create empty location table
    create_location_dimension_query = """
        DROP TABLE IF EXISTS fact;
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.location_dimension;
        
        CREATE TABLE disaster_db.disaster_db_schema.location_dimension
        (
            location_key    SERIAL,
            city            VARCHAR(190),
            province        VARCHAR(50),
            country         VARCHAR(30),
            canada          BOOLEAN,
            PRIMARY KEY (location_key)
        );
    """
    execute_query(create_location_dimension_query)
    # Populate location_dimension
    city_province_tuple_to_id_map = {}
    with open(CSV_FILE_LOCATION, "rb") as csv_file:
        csv_reader = csv.reader(csv_file)
        with open(PROBLEMATIC_PLACES_FILE_LOCATION, "wb") as problematic_rows_file:
            problematic_csv_writer = csv.writer(problematic_rows_file)
            for csv_row in csv_reader:
                place = csv_row[PLACE_INDEX]
                city_province_tuple = get_city_province_tuple_for_place(place)
                if city_province_tuple not in city_province_tuple_to_id_map and city_province_tuple is not None:
                    location_key = execute_query("""
                        INSERT INTO disaster_db.disaster_db_schema.location_dimension(city, province, country, canada)
                        VALUES ('%s', '%s', 'CANADA', TRUE);
                        SELECT location_key
                        FROM disaster_db.disaster_db_schema.location_dimension
                        WHERE city = '%s' AND province = '%s';
                    """ % (city_province_tuple[0], city_province_tuple[1], city_province_tuple[0], city_province_tuple[1],))
                    city_province_tuple_to_id_map[city_province_tuple] = location_key
                elif city_province_tuple is None:
                    problematic_csv_writer.writerow(csv_row)
                    continue
    print_success("Location dimension created")
    return city_province_tuple_to_id_map


# def create_distinct_locations_csv():
#     with open(OLD_LOCATION_FILE_LOCATION, "rb") as csv_file:
#         csv_reader = csv.reader(csv_file)
#         with open(LOCATION_FILE_LOCATION, "wb") as new_csv_file:
#             csv_writer = csv.writer(new_csv_file)
#             distinct_locations = []
#             for old_row in csv_reader:
#                 # OLD_LOCATION_FILE only has one column containing the location
#                 if old_row[0] not in distinct_locations:
#                     distinct_locations.append(old_row[0])
#             for new_row in distinct_locations:
#                 csv_writer.writerow((new_row,))
#     print_success("Created new location file only containing distinct locations")


# def create_city_province_csv():
#     with open(LOCATION_FILE_LOCATION, "rb") as csv_file:
#         csv_reader = csv.reader(csv_file)
#         with open(CITY_PROVINCE_FILE_LOCATION, "wb") as new_csv_file:
#             csv_writer = csv.writer(new_csv_file)
#             with open(PROBLEMATIC_PLACES_FILE_LOCATION, "wb") as problematic_rows_file:
#                 problematic_csv_writer = csv.writer(problematic_rows_file)
#                 distinct_locations = []
#                 possible_province_labels = TO_PROVINCE_CODE_CONVERSION_MAP.keys()
#                 for row in csv_reader:
#                     province = None
#                     city = None
#                     for label in possible_province_labels:
#                         province_string_index = row[0].lower().rfind(label)
#                         if province_string_index >= 0:
#                             province = TO_PROVINCE_CODE_CONVERSION_MAP[label]
#                             city = row[0][:province_string_index].strip()
#                             if (city, province,) not in distinct_locations:
#                                 distinct_locations.append((city, province,))
#                             break
#                     if province is None or city is None:
#                         # Store the row that doesn't fit in model in another csv file so we can look at it manually
#                         problematic_csv_writer.writerow(row)
#                         continue
#                 for new_row in distinct_locations:
#                     csv_writer.writerow(new_row)
#
#     print_success("Created new location file only containing distinct locations")


def get_city_province_tuple_for_place(place):
    # Remove all non utf8 characters
    place = place.decode('utf-8','ignore').encode("utf-8")
    # Put everything to lowercase
    place = place.lower()
    # Remove all commas
    place = place.replace(",", "")
    # Remove all "
    place = place.replace("\"", "")
    # Remove all single quotes
    place = place.replace("'", "")
    # Remove all \
    place = place.replace("\\", "")
    # Remove city (leading with a space)
    place = place.replace(" city", "")
    # Remove city (not leading with a space)
    place = place.replace("city", "")
    possible_province_labels = TO_PROVINCE_CODE_CONVERSION_MAP.keys()
    province = None
    city = None
    for label in possible_province_labels:
        province_string_index = place.lower().rfind(label)
        if province_string_index >= 0:
            province = TO_PROVINCE_CODE_CONVERSION_MAP[label]
            city = place[:province_string_index].strip()
            if len(city) > 60:
                city = city[:58] + ".."
    if city is None or province is None:
        return None
    else:
        if city is None or city == "":
            city = MAIN_CITY_FOR_PROVINCES[province]
        return city, province,


def create_populate_fact_table(city_province_tuple_to_id_map):
    pass


def create_data_mart():
    log("Starting creation of data mart")
    # Start calling create_populate methods here
    create_populate_date_dimension()
    create_summary_dimension()
    create_disaster_dimension()
    create_cost_dimension()
    city_province_tuple_to_id_map = create_populate_location_dimension()
    create_populate_fact_table(city_province_tuple_to_id_map)


# Connection must be closed after everything is said and done, do add or remove anything past this point
CONNECTION.close()
log('Connection closed')
