from unittest import TestCase
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to
import json

from pipeline_functions.composite_transform import CryptoDataTransform


class TestCryptoDataTransform(TestCase):
    def test_expand(self):
        """
        Test that the composite transform returns the expected outputs.
        """
        # Set data to test with - comma separated values in strings
        # 3 individual dates are given in the test data, 3 transactions each, one set being before 2010
        # expect that the result will yield a valid JSON array with 2 objects - "2010-01-09" and "2015-03-09" as dates
        # with 151.00 and 301.00 as total amounts, respectively
        test_csv_rows = [
            '2010-01-09 02:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,100.50',
            '2010-01-09 03:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,50.50',
            '2010-01-09 04:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,10.50',
            '2009-02-09 02:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,550.00',
            '2009-02-09 03:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,550.00',
            '2009-02-09 04:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,550.00',
            '2015-03-09 02:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,200.50',
            '2015-03-09 03:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,100.50',
            '2015-03-09 04:00:00 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,10.50',
        ]
        # Expect an array with a single JSON serialised string inside
        expected_result = [
            json.dumps([{"date": "2010-01-09", "total_amount": 151.00},
                        {"date": "2015-03-09", "total_amount": 301.00}])
        ]
        # setup context manager for testing pipeline object
        with TestPipeline() as p:
            # Testing pipeline steps, load test data as PColl, retrieve outputs
            inputs = p | beam.Create(test_csv_rows)
            output = inputs | CryptoDataTransform()
            # Testing Assertion function from Beam testing module - ensure PColl output matches expectation
            assert_that(
                output,
                equal_to(expected_result)
            )
