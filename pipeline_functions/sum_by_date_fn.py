from apache_beam import CombineFn


class SumByDateFn(CombineFn):
    def create_accumulator(self):
        """
        Creates accumulator for the transaction value field per calendar day.
        """
        accumulator = 0.0
        return accumulator

    def add_input(self, accumulator, input):
        """
        Adds transaction value for each record per calendar day to the accumulator created above.
        """
        return accumulator + input  # Sum

    def merge_accumulators(self, accumulators):
        """
        Sums the accumulators.
        """
        return sum(accumulators)

    def extract_output(self, accumulator):
        """
        Returns single value per each calendar day from the accumulator.
        """
        return accumulator
