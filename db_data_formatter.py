# coding=utf-8
import csv
import psycopg2
import psycopg2.extras
import sys
import json
from datetime import date
import holidays
import traceback

LOGGING_TURNED_ON = False
CONNECTION_STRING = "host='localhost' dbname='disaster_db' user='postgres' password='password'"
NORTH_AMERICAN_HOLIDAYS = holidays.UnitedStates() + holidays.Canada() + holidays.Mexico()
CSV_FILE_LOCATION = "canadian_disaster_database_source_data.csv"
PROBLEMATIC_ROW_FILE_LOCATION = "problematic_rows.csv"
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

RECOGNIZED_COUNTRIES = {
    "usa": "USA",
    "nepal": "NEPAL",
    "japan": "JAPAN",
    "libya": "LIBYA",
    "saudi arabia": "SAUDI ARABIA",
    "iceland": "ICELAND",
    "haiti": "HAITI",
    "ireland": "IRELAND"
}
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
    "newfoundland" : "NL",
    "labrador" : "NL",
    "prince edward island": "PE",
    "maritime provinces": "NS",
    "martime provinces": "NS",
    "nova scotia": "NS",
    "new brunswick": "NB",
    "quebec": "QC",
    "québec": "QC",
    "ontario": "ON",
    "manitoba": "MB",
    "saskatchewan": "SK",
    "prairie provinces" : "AB",
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
SUMMARY_KEYWORD_LIST = [
    "explosion",
    "entombed",
    "killed",
    "severe",
    "large",
    "dead",
    "homeless",
    "injured",
    "collision",
    "avalanche",
    "blew",
    "blizzard",
    "acid",
    "drought",
    "thunderstorm",
    "heavy",
    "failure",
    "evacuation",
    "derailed",
    "arson"
]

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
        if ("SELECT" in query and "INTO" not in query) or "RETURNING" in query:
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
    sqlCommands.remove("")
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
        FROM    disaster_db.disaster_db_schema.date_dimension;
    """
    holiday_dates_list = []
    results = execute_query(get_date_dimension_rows_query)
    for row in results:
        date_dimension_id = row[0]
        is_holiday = row[1] in holidays_list
        holiday_text = holidays_list.get(row[1])
        if is_holiday:
            holiday_text = holiday_text.replace("'", "''")
            if len(holiday_text) > 50:
                holiday_text = holiday_text[:48] + ".."
            holiday_dates_list.append(date_dimension_id)
            update_query = """
                UPDATE  disaster_db.disaster_db_schema.date_dimension
                SET     is_holiday = TRUE,
                        holiday_text = '%s'
                WHERE   date_key = '%s';
            """ % (holiday_text, date_dimension_id)
            try:
                execute_query(update_query)
            except:
                print_stack_trace()
                continue
    print_success('Updated %d dates with holidays out of %d' % (len(holiday_dates_list), len(results)))


def create_populate_date_dimension():
    # Execute create_date_dimension_script
    execute_scripts_from_file("sql_scripts/create_date_dimension.sql")
    # Fill holidays using canadian holiday data
    populate_date_dimension_holidays(NORTH_AMERICAN_HOLIDAYS)
    print_success("Date dimension created and populated")


def create_populate_summary_dimension():
    create_summary_dimension()
    return populate_summary_dimension()


def create_summary_dimension():
    # Create empty summary table
    create_summary_dimension_query = """
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.fact;
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.summary_dimension;
        
        CREATE TABLE disaster_db.disaster_db_schema.summary_dimension
        (
          summary_key   SERIAL,
          summary       TEXT,
          keyword_1     VARCHAR(20),
          keyword_2     VARCHAR(20),
          keyword_3     VARCHAR(20),
          PRIMARY KEY (summary_key)
        );
    """
    execute_query(create_summary_dimension_query)
    print_success("Summary dimension created")


def populate_summary_dimension():
    summary_tuple_to_id_map = {}
    new_rows_count = 0
    with open(CSV_FILE_LOCATION, "rb") as csv_file:
        csv_reader = csv.reader(csv_file)
        # Skip the header
        next(csv_reader, None)
        for row in csv_reader:
            summary_tuple = get_summary_tuple_for_comment(row)
            if summary_tuple not in summary_tuple_to_id_map:
                sql_script = """
                    INSERT INTO disaster_db.disaster_db_schema.summary_dimension(summary, keyword_1, keyword_2, keyword_3)
                    VALUES (
                      %s, %s, %s, %s
                    )
                    RETURNING summary_key;
                """ % summary_tuple
                summary_key = execute_query(sql_script)
                summary_tuple_to_id_map[summary_tuple] = summary_key[0][0]
                new_rows_count += 1
        print_success("Populated summary dimension with %d rows" % new_rows_count)
    return summary_tuple_to_id_map


def get_summary_tuple_for_comment(row):
    comment = row[COMMENT_INDEX]
    comment = comment.decode('utf-8','ignore').encode("utf-8")
    # escape all single quotes
    comment = comment.replace("'", "''")
    if comment == "" or comment is None:
        comment = "NULL"
    else:
        comment = "'" + comment + "'"
    matching_keywords_list = []
    for keyword in SUMMARY_KEYWORD_LIST:
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
    return comment, keyword1, keyword2, keyword3,

def populate_disaster_dimension():
    disaster_tuple_to_id_map = {}
    with open(CSV_FILE_LOCATION, "rb") as csv_file:
        csv_reader = csv.reader(csv_file)
        # Skip header row
        next(csv_reader, None)
        for csv_row in csv_reader:
            disaster_tuple = get_disaster_tuple(csv_row)
            if disaster_tuple is not None and disaster_tuple not in disaster_tuple_to_id_map:
                sql_script = """
                    INSERT INTO disaster_db.disaster_db_schema.disaster_dimension(disaster_type, disaster_subgroup, disaster_group, disaster_category, magnitude, utility_people_affected)
                    VALUES (
                      %s, %s, %s, %s, %s, %s
                    )
                    RETURNING disaster_key;
                """ % disaster_tuple
                disaster_key = execute_query(sql_script)[0][0]
                disaster_tuple_to_id_map[disaster_tuple] = disaster_key
        print_success("Successfully populated disaster dimension")
    return disaster_tuple_to_id_map


def get_disaster_tuple(csv_row):
    disaster_type = csv_row[EVENT_TYPE_INDEX]
    disaster_subgroup = csv_row[EVENT_SUBGROUP_INDEX]
    disaster_group = csv_row[EVENT_GROUP_INDEX]
    disaster_category = csv_row[EVENT_CATEGORY_INDEX]
    magnitude = csv_row[MAGNITUDE_INDEX]
    utility_people_affected = csv_row[UTILITY_PEOPLE_AFFECTED_INDEX]
    # We have checked in our data and disaster_category should have a length of 8 maximum.
    # Every row for which disaster_category has a length more than 10 is actually invalid so we can just ignore it
    if len(disaster_category) > 10:
        return None
    # Our csv file is encoded in latin-1 but our database only accepts utf-8 characters
    disaster_type = disaster_type.decode('utf-8','ignore').encode("utf-8")
    disaster_category = disaster_category.decode('utf-8','ignore').encode("utf-8")
    disaster_group = disaster_group.decode('utf-8','ignore').encode("utf-8")
    disaster_subgroup = disaster_subgroup.decode('utf-8','ignore').encode("utf-8")
    magnitude = magnitude.decode('utf-8','ignore').encode("utf-8")
    utility_people_affected = utility_people_affected.decode('utf-8','ignore').encode("utf-8")

    if disaster_type == "":
        disaster_type = "NULL"
    else:
        disaster_type = "'" + disaster_type.lower().replace("'", "") + "'"
    if disaster_subgroup == "":
        disaster_subgroup = "NULL"
    else:
        disaster_subgroup = "'" + disaster_subgroup.lower().replace("'", "") + "'"
    if disaster_group == "":
        disaster_group = "NULL"
    else:
        disaster_group = "'" + disaster_group.lower().replace("'", "") + "'"
    if disaster_category == "":
        disaster_category = "NULL"
    else:
        disaster_category = "'" + disaster_category.lower().replace("'", "") + "'"
    # Only earthquakes and tsunamis (denoted by the geological disaster_category subgroup)
    if magnitude == "" or disaster_subgroup.lower() != "'geological'":
        magnitude = "NULL"
    else:
        magnitude = "'" + magnitude.lower().replace("'", "") + "'"
    if utility_people_affected == "":
        utility_people_affected = "NULL"
    else:
        utility_people_affected = "'" + utility_people_affected.lower().replace("'", "") + "'"
    return disaster_type, disaster_subgroup, disaster_group, disaster_category, magnitude, utility_people_affected,


def create_populate_disaster_dimension():
    create_disaster_dimension()
    return populate_disaster_dimension()


def create_disaster_dimension():
    # Create empty disaster table
    create_disaster_dimension_query = """
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.fact;
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.disaster_dimension;
        
        CREATE TABLE disaster_db.disaster_db_schema.disaster_dimension
        (
          disaster_key              SERIAL,
          disaster_type             VARCHAR(40),
          disaster_subgroup         VARCHAR(30),
          disaster_group            VARCHAR(15),
          disaster_category         VARCHAR(10),
          magnitude                 DECIMAL(18, 1),
          utility_people_affected   INT,
          PRIMARY KEY (disaster_key)
        );
    """
    execute_query(create_disaster_dimension_query)


def populate_cost_dimension():
    cost_tuple_to_id_map = {}
    with open(CSV_FILE_LOCATION, "rb") as csv_file:
        csv_reader = csv.reader(csv_file)
        # Skip header row
        next(csv_reader, None)
        for csv_row in csv_reader:
            # Get tuple for the row. Method does some data cleaning at the same time
            cost_tuple = get_cost_tuple(csv_row)
            if cost_tuple not in cost_tuple_to_id_map:
                sql_script = """
                    INSERT INTO disaster_db.disaster_db_schema.cost_dimension(estimated_total_cost, normalized_total_cost, federal_dfaa_payments, 
                        provincial_dfaa_payments, provincial_department_payments, municipal_cost, ogd_cost, insurance_payments, ngo_cost)
                    VALUES (
                      %s, %s, %s, %s, %s, %s, %s, %s, %s
                    )
                    RETURNING cost_key;
                """ % cost_tuple
                cost_key = execute_query(sql_script)
                cost_tuple_to_id_map[cost_tuple] = cost_key[0][0]
        print_success("Successfully populated cost dimension")
    return cost_tuple_to_id_map


def get_cost_tuple(csv_row):
    estimated_total_cost = csv_row[ESTIMATED_TOTAL_COST_INDEX]
    normalized_total_cost = csv_row[NORMALIZED_TOTAL_COST_INDEX]
    federal_payments = csv_row[FEDERAL_DFAA_PAYMENTS_INDEX]
    provincial_dfaa_payments = csv_row[PROVINCIAL_DFAA_PAYMENTS]
    provincial_department_payments = csv_row[PROVINCIAL_DEPARTMENT_PAYMENTS_INDEX]
    municipal_cost = csv_row[MUNICIPAL_COSTS_INDEX]
    ogd_cost = csv_row[OGD_COSTS_INDEX]
    insurance_payments = csv_row[INSURANCE_PAYMENTS_INDEX]
    ngo_cost = csv_row[NGO_PAYMENTS_INDEX]
    # Some cleaning for insertion in the db
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
    return (estimated_total_cost, normalized_total_cost, federal_payments, provincial_dfaa_payments,
            provincial_department_payments, municipal_cost, ogd_cost, insurance_payments, ngo_cost,)


def create_populate_cost_dimension():
    create_cost_dimension()
    return populate_cost_dimension()


def create_cost_dimension():
    # Create empty cost table
    create_cost_dimension_query = """
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.fact;
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.cost_dimension;
        
        CREATE TABLE disaster_db.disaster_db_schema.cost_dimension
        (
          cost_key                          SERIAL,
          estimated_total_cost              BIGINT,
          normalized_total_cost             BIGINT,
          federal_dfaa_payments             BIGINT,
          provincial_dfaa_payments          BIGINT,
          provincial_department_payments    BIGINT,
          municipal_cost                    BIGINT,
          ogd_cost                          BIGINT,
          insurance_payments                BIGINT,
          ngo_cost                          BIGINT,
          PRIMARY KEY (cost_key)
        );
    """
    execute_query(create_cost_dimension_query)


# Returns a map that maps every valid place string to its id in the database
# Only populates rows for canadian locations. Non canadian locations will have to be created some other way
def create_populate_location_dimension():
    # Create empty location table
    create_location_dimension_query = """
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.fact;
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
        with open(PROBLEMATIC_PLACES_FILE_LOCATION, "wb") as problematic_places_file:
            problematic_csv_writer = csv.writer(problematic_places_file)
            for csv_row in csv_reader:
                city_province_country_tuple = get_city_province_country_tuple_for_place(csv_row)
                if city_province_country_tuple not in city_province_tuple_to_id_map and city_province_country_tuple is not None:
                    is_canada = "TRUE" if city_province_country_tuple[2] == "CANADA" else "FALSE"
                    location_key = execute_query("""
                        INSERT INTO disaster_db.disaster_db_schema.location_dimension(city, province, country, canada)
                        VALUES ('%s', '%s', '%s', %s)
                        RETURNING location_key;
                    """ % (city_province_country_tuple[0], city_province_country_tuple[1], city_province_country_tuple[2], is_canada,))
                    city_province_tuple_to_id_map[city_province_country_tuple] = location_key[0][0]
                elif city_province_country_tuple is None:
                    problematic_csv_writer.writerow((csv_row[PLACE_INDEX],))
                    continue
    print_success("Location dimension created")
    return city_province_tuple_to_id_map


def get_city_province_country_tuple_for_place(csv_row):
    place = csv_row[PLACE_INDEX]
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
    country = None
    for label in possible_province_labels:
        province_string_index = place.lower().rfind(label)
        if province_string_index >= 0:
            province = TO_PROVINCE_CODE_CONVERSION_MAP[label]
            city = place[:province_string_index].strip()
            country = "CANADA"
            if len(city) > 60:
                city = city[:58] + ".."
    if province is None:
        for recognized_country in RECOGNIZED_COUNTRIES:
            country_string_index = place.lower().rfind(recognized_country)
            if country_string_index >= 0:
                province = RECOGNIZED_COUNTRIES[recognized_country]
                city = RECOGNIZED_COUNTRIES[recognized_country]
                country = RECOGNIZED_COUNTRIES[recognized_country]

    if city is None and province is None:
        return None
    else:
        if city is None or city == "":
            city = MAIN_CITY_FOR_PROVINCES[province]
        return city, province, country,


def create_fact_table():
    create_fact_table_query = """
        DROP TABLE IF EXISTS disaster_db.disaster_db_schema.fact;
        CREATE TABLE disaster_db.disaster_db_schema.fact
        (
            start_date_key INT REFERENCES disaster_db.disaster_db_schema.date_dimension(date_key),
            end_date_key INT REFERENCES disaster_db.disaster_db_schema.date_dimension(date_key),
            location_key INT REFERENCES disaster_db.disaster_db_schema.location_dimension(location_key),
            disaster_key INT REFERENCES disaster_db.disaster_db_schema.disaster_dimension(disaster_key),
            summary_key INT REFERENCES disaster_db.disaster_db_schema.summary_dimension(summary_key),
            cost_key INT REFERENCES disaster_db.disaster_db_schema.cost_dimension(cost_key),
            fatality_number DECIMAL,
            injured_number DECIMAL,
            evacuated_number DECIMAL,
            -- days_between_sighting_and_posting INT,
            PRIMARY KEY (start_date_key, end_date_key, location_key, disaster_key, summary_key)
        );
    """
    execute_query(create_fact_table_query)
    print_success("Successfully created fact table")

def create_populate_fact_table(city_province_tuple_to_id_map, cost_tuple_to_id_map, disaster_tuple_to_id_map, summary_tuple_to_id_map):
    create_fact_table()
    with open(CSV_FILE_LOCATION, "rb") as csv_file:
        csv_reader = csv.reader(csv_file)
        with open(PROBLEMATIC_ROW_FILE_LOCATION, "wb") as problematic_csv_file:
            csv_writer = csv.writer(problematic_csv_file)
            next(csv_reader, None)
            for csv_row in csv_reader:
                try:
                    # Get key from tuple to id maps when possible
                    cost_tuple = get_cost_tuple(csv_row)
                    cost_key = cost_tuple_to_id_map[cost_tuple]
                    city_province_country_tuple = get_city_province_country_tuple_for_place(csv_row)
                    location_key = city_province_tuple_to_id_map[city_province_country_tuple]
                    disaster_tuple = get_disaster_tuple(csv_row)
                    disaster_key = disaster_tuple_to_id_map[disaster_tuple]
                    summary_tuple = get_summary_tuple_for_comment(csv_row)
                    summary_key = summary_tuple_to_id_map[summary_tuple]
                    # For date dimension, we need to run a query to get the start_date_key
                    start_date = csv_row[EVENT_START_DATE_INDEX].split(" ")[0]
                    start_date_query_result = execute_query("""
                        SELECT  date_key FROM disaster_db.disaster_db_schema.date_dimension
                        WHERE   date_actual = TO_DATE('%s', 'MM/DD/YYYY') LIMIT 1;
                    """ % start_date)
                    if len(start_date_query_result) == 1:
                        start_date_key = start_date_query_result[0][0]
                    else:
                        print "no start date: " + csv_row
                        continue
                    # For date dimension, we need to run a query to get the end_date_key
                    end_date = csv_row[EVENT_END_DATE_INDEX].split(" ")[0]
                    end_date_query_result = execute_query("""
                        SELECT  date_key FROM disaster_db.disaster_db_schema.date_dimension
                        WHERE   date_actual = TO_DATE('%s', 'MM/DD/YYYY') LIMIT 1;
                    """ % end_date)
                    if len(end_date_query_result) == 1:
                        end_date_key = end_date_query_result[0][0]
                    else:
                        print "no end date: " + csv_row
                        continue
                    # That's it for getting the keys, now we get the facts/measures
                    fatality_number = csv_row[FATALITIES_INDEX]
                    if fatality_number == "":
                        fatality_number = "NULL"
                    injured_number = csv_row[INJURED_INFECTED_INDEX]
                    if injured_number == "":
                        injured_number = "NULL"
                    evacuated_number = csv_row[EVACUATED_INDEX]
                    if evacuated_number == "":
                        evacuated_number = "NULL"
                    # Finally, let's try inserting the row into the fact table
                    insert_query = """
                        INSERT INTO disaster_db.disaster_db_schema.fact(start_date_key, end_date_key, location_key, disaster_key, summary_key, cost_key, fatality_number, injured_number, evacuated_number)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """ % (start_date_key, end_date_key, location_key, disaster_key, summary_key, cost_key, fatality_number, injured_number, evacuated_number)
                    execute_query(insert_query)
                except:
                    # Write row causing a problem to a csv file and continue
                    csv_writer.writerow(csv_row)
                    continue
            print_success("Successfully populated fact table")


def create_data_mart():
    log("Starting creation of data mart")
    # Start calling create_populate methods here
    create_populate_date_dimension()
    summary_tuple_to_id_map = create_populate_summary_dimension()
    disaster_tuple_to_id_map = create_populate_disaster_dimension()
    cost_tuple_to_id_map = create_populate_cost_dimension()
    city_province_tuple_to_id_map = create_populate_location_dimension()
    create_populate_fact_table(city_province_tuple_to_id_map, cost_tuple_to_id_map, disaster_tuple_to_id_map, summary_tuple_to_id_map)


create_data_mart()
# Connection must be closed after everything is said and done, do add or remove anything past this point
CONNECTION.close()
log('Connection closed')
