import apache_beam as beam
import json

from pipeline_functions.transforms_and_filters import parse_csv, filter_after_year, datetime_conversion, \
    convert_to_key_value, convert_to_dict
from pipeline_functions.sum_by_date_fn import SumByDateFn


class CryptoDataTransform(beam.PTransform):
    def expand(self, p):
        a = (
            p
            | 'SplitCSVLines' >> beam.Map(parse_csv)
            | 'ValueGreaterThan20' >> beam.Filter(lambda x: x[3] > 20)  # Filter for transactions greater than 20
            | 'Filter2010' >> beam.Filter(filter_after_year, year=2010)  # Exclude all transactions made before 2010
            | 'DatetimeConversion' >> beam.Map(datetime_conversion)  # Convert timestamp into date
            | 'ConvertToKeyValuePair' >> beam.Map(convert_to_key_value)  # Convert list into tuple of len 2
            | 'SumTotalByDate' >> beam.CombinePerKey(SumByDateFn())  # Sum total by date, return date & total amount
            | 'ConvertToDict' >> beam.Map(convert_to_dict)  # Convert resulting aggregated tuples into dicts
            | 'ToDictArray' >> beam.combiners.ToList()
            | 'SerialiseJSON' >> beam.Map(json.dumps)  # Serialise Dict object as a JSON string
        )
        return a
