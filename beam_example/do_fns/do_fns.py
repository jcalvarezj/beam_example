# User-defined DoFns

import apache_beam as beam


class FilterOutHeader(beam.DoFn):
    """
        DoFn that only returns elements that don't match with the CSV header
    """
    def __init__(self, expected_header_prefix):
        self.expected_header_prefix = expected_header_prefix

    def process(self, element):
        """
            Returns the element only if it doesn't match with the CSV header
        """
        if not element.startswith(self.expected_header_prefix):
            yield element
