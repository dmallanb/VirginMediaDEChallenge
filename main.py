import json

import apache_beam as beam
import logging

from pipeline_functions.transforms_and_filters import parse_csv, filter_after_year, datetime_conversion, \
    convert_to_key_value, convert_to_dict
from pipeline_functions.sum_by_date_fn import SumByDateFn
from pipeline_functions.composite_transform import CryptoDataTransform

####################
# Pipeline Options #
####################
# These pipeline options can be done using the PipelineOptions class from the beam library, however given the static
# nature of this challenge without need for complex configuration, I have hard coded the options as variables instead
# for simplicity
input_file = "gs://cloud-samples-data/bigquery/sample-transactions/transactions.csv"
output_dir = "output"


################################
# TASK 1 - MULTI STEP PIPELINE #
################################
with beam.Pipeline() as pipeline:  # options=beam_options
    # Begin Pipeline - pull CSV from GCS, skipping headers
    p = (
        pipeline
        | 'ReadFromGCS' >> beam.io.ReadFromText(input_file,
                                                skip_header_lines=1)
        | 'SplitCSVLines' >> beam.Map(parse_csv)  # Split each CSV row string into strings/float for filtering
    )
    # Run Filters
    filtered = (
        p
        | 'ValueGreaterThan20' >> beam.Filter(lambda x: x[3] > 20)  # Filter for transactions greater than 20
        | 'Filter2010' >> beam.Filter(filter_after_year, year=2010)  # Exclude all transactions made before 2010
    )
    # Run Transform steps
    transforms = (
        filtered
        | 'DatetimeConversion' >> beam.Map(datetime_conversion)  # Convert timestamp into date
        | 'ConvertToKeyValuePair' >> beam.Map(convert_to_key_value)  # Convert list into tuple of len 2
        | 'SumTotalByDate' >> beam.CombinePerKey(SumByDateFn())  # Sum total by date, return date & total amount
        | 'ConvertToDict' >> beam.Map(convert_to_dict)  # Convert resulting aggregated tuples into dicts
        | 'ToDictArray' >> beam.combiners.ToList()  # Combine PCollection dicts into a list for JSON conversion
        | 'SerialiseJSON' >> beam.Map(json.dumps)  # Serialise array of dicts as a JSON string
    )
    # Output JSON file with GZIP compression
    output = transforms | "WriteOut" >> beam.io.WriteToText(file_path_prefix=f"{output_dir}/task1",
                                                            file_name_suffix=".json.gz",
                                                            compression_type="gzip")

##############################################
# TASK 2 - PIPELINE WITH COMPOSITE TRANSFORM #
##############################################
with beam.Pipeline() as pipeline:  # options=beam_options
    logging.info("Starting Task 2 Pipeline - With Composite Transform")
    p = pipeline | beam.io.ReadFromText(input_file, skip_header_lines=1)
    # Run composite transform
    composite_transform = p | "TransformAndFilter" >> CryptoDataTransform()
    # Output resulting data
    output = composite_transform | "WriteOut" >> beam.io.WriteToText(file_path_prefix=f"{output_dir}/task2",
                                                                     file_name_suffix=".json.gz",
                                                                     compression_type="gzip")
