from unittest import TestCase
import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

from pipeline_functions.sum_by_date_fn import SumByDateFn


class TestSumByDateFn(TestCase):
    def test_sum_by_date(self):
        """
        Test that the by-key sum aggregation transform returns the expected outputs.
        """
        # Generate test data - this function expects a tuple of len 2, with date as the key, and amount as the value.
        test_data = [
            ("2015-01-01", 50.50),
            ("2015-01-01", 100.00),
            ("2015-01-01", 49.50),
            ("2016-01-01", 20.00),
            ("2016-01-01", 20.00),
            ("2016-01-01", 20.00)
        ]
        # setup context manager for testing pipeline object
        with TestPipeline() as p:
            # Testing pipeline steps, load test data as PColl, retrieve outputs
            inputs = p | beam.Create(test_data)
            output = inputs | beam.CombinePerKey(SumByDateFn())
            # Testing Assertion function from Beam testing module - ensure PColl output matches expectation
            assert_that(
                output,
                equal_to([
                    ("2015-01-01", 200.00),
                    ("2016-01-01", 60.00)
                ])
            )

