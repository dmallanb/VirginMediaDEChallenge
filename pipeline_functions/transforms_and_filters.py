import csv
from datetime import datetime


##############
# Transforms #
##############
def parse_csv(element):
    """
    Function to convert a CSV line stored as text by Apache Beam into python standard objects (list, str, float).
    :param str element: Line element from a CSV file, passed in by Beam.
    :return list line: A row of CSV, parsed as a list.
    """
    try:
        line = element.split(',')
        # CSV line comes in as a string - convert value to float
        line[3] = float(line[3])
        return line
    except Exception:
        print("WARNING - Invalid data passed for amount - check that CSV headers are not passed into the PCollection")


def datetime_conversion(element):
    """
    Converts timestamp strings to date strings.
    :param list element: Line list element from a CSV file, passed in by Beam.
    :return list element: Line list element from a CSV file.
    """
    datetime_obj = datetime.strptime(element[0], "%Y-%m-%d %H:%M:%S %Z")
    element[0] = datetime_obj.strftime("%Y-%m-%d")
    return element


def convert_to_key_value(element):
    """
    To convert the row array into a key value pair for date/price.
    :param list element: Line list element from a CSV file, passed in by Beam.
    :return tuple: Date and price as a tuple from the element.
    """
    return element[0], element[3]


def convert_to_dict(element):
    """
    Converts a Tuple to a Dict.
    :param tuple element: 2-item tuple element with date and total amount as items.
    :return dict: Dict containing the Tuple as a dict with headers applied as keys.
    """
    return {"date": element[0], "total_amount": element[1]}


###########
# Filters #
###########
def filter_after_year(element, year: int):
    """
    Filters elements where the timestamp is before the given year.
    2010.
    :param list element: Line list element from a CSV file, passed in by Beam.
    :param int year: Year to filter entries after
    :return list element: Line list element from a CSV file.
    """
    record_date = datetime.strptime(element[0], "%Y-%m-%d %H:%M:%S %Z")
    if record_date.year >= year:
        return element
