import csv
import psycopg2
import psycopg2.extras
import sys
import pprint
from datetime import date
import holidays
import traceback

REPORTED_DATETIME_INDEX = 0
CITY_INDEX = 1
STATE_INDEX = 2
SHAPE_INDEX = 3
DURATION_INDEX = 4
SUMMARY_INDEX = 5
POSTED_DATE_INDEX = 6
CONNECTION_STRING = "host='localhost' dbname='postgres' user='postgres' password='safyrya89'"
us_holidays = holidays.UnitedStates()
CSV_FILE_LOCATION = "nuforcScrape.csv"


class MissingDimensionValueException(Exception):
    pass

class DuplicateRowException(Exception):
    pass


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


def date_to_string(date):
    return "{:%m-%d-%Y}".format(date)


def create_summary_dimension(csvLocation):
    with open(csvLocation, "rb") as csvFile:
        csvReader = csv.reader(csvFile)
        # Create empty summary table
        create_summary_dimension_query = """
            DROP TABLE IF EXISTS ufo_fact;
            DROP TABLE IF EXISTS summary_dimension;
            
            CREATE TABLE summary_dimension
            (
              summary_key   SERIAL,
              summary       VARCHAR(200),
              PRIMARY KEY (summary_key)
            );
        """
        results = execute_query(create_summary_dimension_query)


def correct_country_in_location_dimension():
    correct_canada_country_query = """
        UPDATE  public.location_dimension
        SET     country = 'canada'
        WHERE   city LIKE '%canada%';
    """
    execute_query(correct_canada_country_query)
    correct_greece_country_query = """
        UPDATE  public.location_dimension
        SET     country = 'greece'
        WHERE   city LIKE '%greece%';
    """
    execute_query(correct_greece_country_query)
    correct_bulgaria_country_query = """
        UPDATE  public.location_dimension
        SET     country = 'bulgaria'
        WHERE   city LIKE '%bulgaria%';
    """
    execute_query(correct_bulgaria_country_query)
    correct_uk_country_query = """
        UPDATE  public.location_dimension
        SET     country = 'uk/england'
        WHERE   city LIKE '%united kingdom%';
    """
    execute_query(correct_uk_country_query)
    correct_mexico_country_query = """
        UPDATE  public.location_dimension
        SET     country = 'mexico'
        WHERE   city LIKE '%mexico%';
    """
    execute_query(correct_mexico_country_query)
    correct_germany_country_query = """
        UPDATE  public.location_dimension
        SET     country = 'germany'
        WHERE   city LIKE '%germany%';
    """
    execute_query(correct_germany_country_query)
    correct_japan_country_query = """
        UPDATE  public.location_dimension
        SET     country = 'japan'
        WHERE   city LIKE '%japan%';
    """
    execute_query(correct_japan_country_query)
    correct_india_country_query = """
        UPDATE  public.location_dimension
        SET     country = 'india'
        WHERE   city LIKE '%india%';
    """
    execute_query(correct_india_country_query)


def create_location_dimension(csvLocation):
    with open(csvLocation, "rb") as csvFile:
        csvReader = csv.reader(csvFile)
        # Create empty location table
        create_location_dimension_query = """
            DROP TABLE IF EXISTS location_dimension;
            
            CREATE TABLE location_dimension
            (
              location_key  SERIAL,
              city          VARCHAR(70),
              state         VARCHAR(5),
              country       VARCHAR(20) DEFAULT 'united states',
              region        VARCHAR(20) DEFAULT 'north america',
              PRIMARY KEY (location_key)
            );
        """
        execute_query(create_location_dimension_query)
        locationList = []
        for row in csvReader:
            tmp_location_tuple = (row[CITY_INDEX].lower().replace("'", "''"), row[STATE_INDEX].lower().replace("'", "''"))
            if tmp_location_tuple not in locationList:
                locationList.append(tmp_location_tuple)
                print "Added (%s, %s) to location dimension" % (row[CITY_INDEX], row[STATE_INDEX])
        if len(locationList) > 0:
            insert_location_data_string = "("
            location_string_list = []
            for location_tuple in locationList:
                if location_tuple[0] == '' and location_tuple[1] != '':
                    location_string_list.append("NULL, '%s'" % location_tuple[1])
                if location_tuple[0] != '' and location_tuple[1] == '':
                    location_string_list.append("'%s', NULL" % location_tuple[0])
                if location_tuple[0] == '' and location_tuple[1] == '':
                    location_string_list.append("NULL, NULL")
                else:
                    location_string_list.append("'%s', '%s'" % location_tuple)
            insert_location_data_string += "),\n(".join(location_string_list)
            insert_location_data_string += ")\n"

            # Fill up location table with data from the csv
            insert_into_location_dim_query = """
                INSERT INTO public.location_dimension(city, state)
                VALUES %s;
            """ % insert_location_data_string
            execute_query(insert_into_location_dim_query)
            correct_country_in_location_dimension()


def get_all_distinct_shapes(csvLocation):
    with open(csvLocation, "rb") as csvFile:
        csvReader = csv.reader(csvFile)
        shape_name_list = []
        for row in csvReader:
            shape_name = row[SHAPE_INDEX].lower() if row[SHAPE_INDEX] is not None else row[SHAPE_INDEX]
            if shape_name not in shape_name_list \
                    and shape_name not in ['unknown', 'others', 'other', '']:
                shape_name_list.append(shape_name.lower())
        return shape_name_list


def create_shape_dimension():
    print "Connecting to database with the following connection string\n->%s" % CONNECTION_STRING
    connection = psycopg2.connect(CONNECTION_STRING)
    print "Connected"

    # Configure cursor
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    
    create_shape_dimension_query = """
            DROP TABLE IF EXISTS shape_dimension;
            
            CREATE TABLE shape_dimension
            (
              shape_key SERIAL,
              shape_name VARCHAR(15),
              PRIMARY KEY (shape_key)
            );
    """
    try:
        cursor.execute(create_shape_dimension_query)
        shape_name_list = get_all_distinct_shapes(CSV_FILE_LOCATION)
        if len(shape_name_list) > 0:
            added_rows_script = "('"
            added_rows_script += "'),\n('".join(shape_name_list)
            added_rows_script += "')\n"
            # Query to create shape dimension
            # We add a null shape at the end for missing values
            insert_query = """
                INSERT INTO shape_dimension(shape_name)
                VALUES %s, (NULL);
            """ % added_rows_script
            cursor.execute(insert_query)
            print "shape dimension created"

    finally:
        cursor.close()
        connection.commit()
        connection.close()
        print 'Connection closed'


def populate_reported_date_dimension_holidays():
    # gets all rows in the reported date dimension
    get_reported_date_rows_query = """
        SELECT  reported_date_key,
                date_actual
        FROM    public.reported_date_dimension;
    """

    holiday_dates_list = []

    results = execute_query(get_reported_date_rows_query)
    for row in results:
        date_dimension_id = row[0]
        is_holiday = row[1] in us_holidays
        holiday_text = us_holidays.get(row[1])
        if is_holiday:
            holiday_text = holiday_text.replace("'", "''")
            holiday_dates_list.append(date_dimension_id)
            update_query = """
                UPDATE  public.reported_date_dimension
                SET     is_holiday = TRUE,
                        holiday_text = '%s'
                WHERE   reported_date_key = '%s';
            """ % (holiday_text, date_dimension_id)
            execute_query(update_query)
    print 'Updated %d dates out of %d' % (len(holiday_dates_list), len(results))


def populate_posted_date_dimension_holidays():
    # gets all rows in the reported date dimension
    get_posted_date_rows_query = """
        SELECT  posted_date_key,
                date_actual
        FROM    public.posted_date_dimension;
    """

    holiday_dates_list = []

    results = execute_query(get_posted_date_rows_query)
    for row in results:
        date_dimension_id = row[0]
        is_holiday = row[1] in us_holidays
        holiday_text = us_holidays.get(row[1])
        if is_holiday:
            holiday_text = holiday_text.replace("'", "''")
            holiday_dates_list.append(date_dimension_id)
            update_query = """
                UPDATE  public.posted_date_dimension
                SET     is_holiday = TRUE,
                        holiday_text = '%s'
                WHERE   posted_date_key = '%s';
            """ % (holiday_text, date_dimension_id)
            execute_query(update_query)
    print 'Updated %d dates out of %d' % (len(holiday_dates_list), len(results))


# Read a csv file to another
def readCSVToOtherCSV(csvLocation):
    with open(csvLocation, "rb") as csvFile:
        with open("new_" + csvLocation, "wb") as newCsvFile:
            csvReader = csv.reader(csvFile)
            csvWriter = csv.writer(newCsvFile)
            for row in csvReader:
                print ', '.join(row)
                csvWriter.writerow(row)


# Read from postgres database
def createTableAndReadTable():

    createTableQuery = """
        CREATE TABLE public.test2
        (
          id  VARCHAR(10) NOT NULL
              CONSTRAINT test2_pkey
              PRIMARY KEY,
          content2 VARCHAR
        );
    """
    execute_query(createTableQuery)

    # Perform SELECT query
    select_query = "SELECT * FROM public.test2;"
    records = execute_query(select_query)


def execute_query(query):
    print "Connecting to database with the following connection string\n->%s" % CONNECTION_STRING
    connection = psycopg2.connect(CONNECTION_STRING)
    print "Connected"
    # Configure cursor
    cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
    results = []
    try:
        cursor.execute(query)
        if "SELECT" in cursor.statusmessage and "INTO" not in cursor.statusmessage:
            results = cursor.fetchall()
        print "Query successful"
    except:
        print bcolors.FAIL
        traceback.print_exc()
        print bcolors.ENDC
    finally:
        cursor.close()
        connection.commit()
        connection.close()
        print 'Connection closed'
        return results


def populate_fact_table_and_summary(csv_location):
    with open(csv_location, "rb") as csvFile:
        csvReader = csv.reader(csvFile)
        print "Connecting to database with the following connection string\n->%s" % CONNECTION_STRING
        connection = psycopg2.connect(CONNECTION_STRING)
        print "Connected"
        # Configure cursor
        cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
        results = []
        try:
            #All the code to populate fact table starts here
            create_fact_table_query = """
                DROP TABLE IF EXISTS UFO_Fact;
                CREATE TABLE UFO_Fact
                (
                    summary_key INT REFERENCES summary_dimension(summary_key),
                    shape_key INT REFERENCES shape_dimension(shape_key),
                    reported_date_key INT REFERENCES reported_date_dimension(reported_date_key),
                    posted_date_key INT REFERENCES posted_date_dimension(posted_date_key),
                    location_key INT REFERENCES location_dimension(location_key),
                    duration_text text,
                    duration_sec DECIMAL,
                    duration_minute DECIMAL,
                    -- days_between_sighting_and_posting INT,
                    PRIMARY KEY (summary_key, shape_key, reported_date_key, posted_date_key, location_key)
                );
            """
            cursor.execute(create_fact_table_query)
            print bcolors.OKGREEN + 'Created UFO_Fact table' + bcolors.ENDC
            # Iterate over each row in the csv file,
            # add summary to summary dimension if not there,
            # Find correct dimension keys and use them to add row to fact table
            # Extract numerical values out of duration_text
            for csv_row in csvReader:
                # Extract interesting values out of csv row
                summary_text = csv_row[SUMMARY_INDEX].replace("'", "''")
                duration_text = csv_row[DURATION_INDEX]
                posted_date = csv_row[POSTED_DATE_INDEX]
                reported_datetime = csv_row[REPORTED_DATETIME_INDEX]
                shape_text = csv_row[SHAPE_INDEX].lower()
                city_text = csv_row[CITY_INDEX].lower().replace("'", "''")
                state_text = csv_row[STATE_INDEX].lower().replace("'", "''")
                # Keys to use to link fact to dimensions
                # To be populated as the code goes on
                summary_key = None
                reported_date_key = None
                location_key = None
                shape_key = None

                # Get summary_key
                get_summary_key_query = ""
                if summary_text == '' or summary_text is None:
                    get_summary_key_query = """
                        SELECT  summary_key
                        FROM    summary_dimension
                        WHERE   summary IS NULL;
                    """
                else:
                    get_summary_key_query = """
                        SELECT  summary_key
                        FROM    summary_dimension
                        WHERE   summary = '%s';
                    """ % summary_text
                cursor.execute(get_summary_key_query)
                summary_key_set = cursor.fetchall()
                if len(summary_key_set) == 0:
                    # No key found, create row
                    create_summary_row_query = ""
                    if summary_text is None:
                        create_summary_row_query = """
                            INSERT INTO summary_dimension(summary)
                            VALUES (NULL);
                        """
                    else:
                        create_summary_row_query = """
                            INSERT INTO summary_dimension(summary)
                            VALUES ('%s');
                        """ % summary_text
                    cursor.execute(create_summary_row_query)
                    connection.commit()
                    # Repeat prior select query to get the key of the new summary row
                    cursor.execute(get_summary_key_query)
                    summary_key_set = cursor.fetchall()
                    print get_summary_key_query
                    summary_key = summary_key_set[0][0]
                elif len(summary_key_set) == 1:
                    # Exactly one key found, get the key out and use it
                    summary_key = summary_key_set[0][0]
                else:
                    # More than 1 key was found, meaning duplicate data was inserted in the database,
                    # throw exception and fix code so that this can't happen
                    continue
                    # raise DuplicateRowException("More than one summary row with the same text found")

                # Get reported date key
                get_reported_date_key_query = """
                    SELECT  reported_date_key
                    FROM    reported_date_dimension
                    WHERE   date_actual = to_date('%s', 'YYYY-MM-DD');
                """ % reported_datetime[:10]
                cursor.execute(get_reported_date_key_query)
                reported_date_key = cursor.fetchone()
                if reported_date_key is None:
                    print "Summary of row causing an issue: " + summary_text
                    continue
                    # raise MissingDimensionValueException("reported_date not found in reported_date_dimension")
                else:
                    reported_date_key = reported_date_key[0]

                # Get posted date key
                get_posted_date_key_query = """
                    SELECT  posted_date_key
                    FROM    posted_date_dimension
                    WHERE   date_actual = to_date('%s', 'YYYY-MM-DD');
                """ % posted_date[:10]
                cursor.execute(get_posted_date_key_query)
                posted_date_key = cursor.fetchone()
                if posted_date_key is None:
                    print "posted_date: " + posted_date
                    print "Summary of row causing an issue: " + summary_text
                    # Use reported date for posted date
                    get_posted_date_key_with_reported_date_query = """
                        SELECT  posted_date_key
                        FROM    posted_date_dimension
                        WHERE   date_actual = to_date('%s', 'YYYY-MM-DD');
                    """ % reported_datetime[:10]
                    cursor.execute(get_posted_date_key_query)
                    posted_date_key = cursor.fetchone()
                    if posted_date_key is None:
                        pass
                    else:
                        posted_date_key = posted_date_key[0]
                    # raise MissingDimensionValueException("posted_date not found in posted_date_dimension")
                else:
                    posted_date_key = posted_date_key[0]

                # Get location_key
                get_location_key_query = ""
                if city_text == '' and state_text != '':
                    get_location_key_query = """
                        SELECT  location_key
                        FROM    location_dimension
                        WHERE   city IS NULL AND state = '%s';
                    """ % state_text
                if city_text != '' and state_text == '':
                    get_location_key_query = """
                        SELECT  location_key
                        FROM    location_dimension
                        WHERE   city = '%s' AND state IS NULL;
                    """ % city_text
                if city_text == '' and state_text == '':
                    get_location_key_query = """
                        SELECT  location_key
                        FROM    location_dimension
                        WHERE   city IS NULL AND state IS NULL;
                    """
                else:
                    get_location_key_query = """
                        SELECT  location_key
                        FROM    location_dimension
                        WHERE   city = '%s' AND state = '%s';
                    """ % (city_text, state_text)
                cursor.execute(get_location_key_query)
                location_key = cursor.fetchone()
                if location_key is None:
                    print "Summary of row causing an issue: " + summary_text
                    continue
                    # raise MissingDimensionValueException("location not found in location_dimension")
                else:
                    location_key = location_key[0]

                # Get shape_key
                if shape_text in ('unknown', 'other', 'others', ''):
                    shape_text = None
                get_shape_key_query = ""
                if shape_text is None:
                    get_shape_key_query = """
                        SELECT  shape_key
                        FROM    shape_dimension
                        WHERE   shape_name IS NULL;
                    """
                else:
                    get_shape_key_query = """
                        SELECT  shape_key
                        FROM    shape_dimension
                        WHERE   shape_name = '%s';
                    """ % shape_text
                cursor.execute(get_shape_key_query)
                shape_key = cursor.fetchone()
                if shape_key is None:
                    print "Summary of row causing an issue: " + summary_text
                    continue
                    # raise MissingDimensionValueException("shape not found in shape_dimension")
                else:
                    shape_key = shape_key[0]

                try:
                    # Finally, use the keys to insert a row into the fact table
                    # But first, check if there exists a fact row for the primary keys we have
                    check_fact_row_query = """
                        SELECT duration_text
                        FROM UFO_Fact
                        WHERE summary_key = %d 
                        and   shape_key = %d
                        and   reported_date_key = %d
                        and   posted_date_key = %d
                        and   location_key = %d
                    """ % (summary_key, shape_key, reported_date_key, posted_date_key, location_key)
                    cursor.execute(check_fact_row_query)
                    existing_row_record = cursor.fetchall()
                    if len(existing_row_record) > 0:
                        # Duplicate row found in the CSV file. Might be in the same day but at different times.
                        # Since it is weird for the summary to be the same we ignore such data
                        print summary_text
                    else :
                        duration_sec = get_duration_sec_from_duration_text(duration_text)
                        duration_minute = duration_sec/60
                        create_fact_row_query = """
                            INSERT INTO UFO_Fact
                            (summary_key, shape_key, reported_date_key, posted_date_key, location_key, duration_text, 
                            duration_minute, duration_sec)
                            VALUES (%d,%d,%d,%d,%d,'%s',%d,%d);
                        """ % (summary_key, shape_key, reported_date_key, posted_date_key, location_key, duration_text, duration_minute, duration_sec)
                        cursor.execute(create_fact_row_query)
                except:
                    continue

            print bcolors.OKBLUE + "Populated UFO_Fact table" + bcolors.FAIL

        except MissingDimensionValueException:
            print bcolors.FAIL
            print "Caught MissingDimensionValueException"
            traceback.print_exc()
            print bcolors.ENDC
        except DuplicateRowException:
            print bcolors.FAIL
            print "Caught DuplicateRowException"
            traceback.print_exc()
            print bcolors.ENDC
        except:
            print bcolors.FAIL
            traceback.print_exc()
            print bcolors.ENDC
        finally:
            cursor.close()
            connection.commit()
            connection.close()
            print 'Connection closed'
            return results


def get_duration_sec_from_duration_text(duration_text):
    first_float = None
    for t in duration_text.split():
        try:
            first_float = float(t)
            break
        except ValueError:
            pass
    if 'sec' in duration_text:
        return first_float
    elif 'min' in duration_text:
        return first_float * 60
    elif 'hour' in duration_text:
        return first_float * 60 * 60
    else:
        print bcolors.WARNING + "Following duration_text was unconvertible: " + duration_text + bcolors.ENDC
        return 0


# For this method to work, the create_postgres_posted_date.sql create_postgres_reported_date.sql
# SQL scripts need to have been run
def create_data_mart():
    populate_posted_date_dimension_holidays()
    populate_reported_date_dimension_holidays()
    create_shape_dimension()
    create_summary_dimension(CSV_FILE_LOCATION)
    create_location_dimension(CSV_FILE_LOCATION)
    populate_fact_table_and_summary(CSV_FILE_LOCATION)

create_data_mart()