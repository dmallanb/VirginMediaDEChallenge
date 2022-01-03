from unittest import TestCase

from pipeline_functions.transforms_and_filters import parse_csv, filter_after_year, datetime_conversion, \
    convert_to_key_value, convert_to_dict


class TestPipelineTransformsAndFilters(TestCase):
    """
    Unit tests for enabling Test Driven Development (TDD) of individual pipeline steps.
    """
    def test_parse_file(self):
        """
        Test that each row of the 4 column CSV file can be parsed correctly.
        """
        test_row = '2009-01-09 02:54:25 UTC,wallet00000e719adfeaa64b5a,wallet00001866cb7e0f09a890,1021101.99'
        expected = ["2009-01-09 02:54:25 UTC", "wallet00000e719adfeaa64b5a", "wallet00001866cb7e0f09a890", 1021101.99]
        self.assertListEqual(parse_csv(test_row), expected)
        # Negative test - test that header row is not returned if it is not skipped
        test_row = 'timestamp,from,to,amount'
        self.assertIsNone(parse_csv(test_row))

    def test_filter_after_year(self):
        """
        Test that entries taking place before a given year will be filtered out of a PColl.
        """
        year = 2010
        # test with row with transaction after 2010
        test_row = ["2017-01-01 04:22:23 UTC", "wallet00000e719adfeaa64b5a", "wallet00001e494c12b3083634", 19.95]
        self.assertListEqual(test_row, filter_after_year(test_row, year))
        # test with row with transaction before 2010 - expect no return
        test_row = ["2009-01-09 04:22:23 UTC", "wallet00000e719adfeaa64b5a", "wallet00001866cb7e0f09a890", 1021101.99]
        self.assertIsNone(filter_after_year(test_row, year))

    def test_datetime_conversion(self):
        """
        Test that the timestamp string can be successfully reformatted into a date string for a PColl element.
        """
        test_row = ["2017-01-01 04:22:23 UTC", "wallet00000e719adfeaa64b5a", "wallet00001e494c12b3083634", 19.95]
        expected = ["2017-01-01", "wallet00000e719adfeaa64b5a", "wallet00001e494c12b3083634", 19.95]
        self.assertListEqual(expected, datetime_conversion(test_row))

    def test_convert_to_key_value(self):
        """
        Test that a CSV row, represented as a list can be truncated to a 2-element tuple (key, value) for later
        aggregation.
        """
        test_row = ["2017-01-01", "wallet00000e719adfeaa64b5a", "wallet00001e494c12b3083634", 19.95]
        expected = ("2017-01-01", 19.95)
        self.assertEqual(expected, convert_to_key_value(test_row))

    def test_convert_to_dict(self):
        """
        Test that a Tuple can be converted to a Dict with the correct keys.
        """
        test_row = ("2017-01-01", 19.95)
        expected = {"date": "2017-01-01", "total_amount": 19.95}
        self.assertDictEqual(expected, convert_to_dict(test_row))
