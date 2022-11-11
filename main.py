import apache_beam as beam


MAKE_IDX = 0
PRICE_IDX = 2


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


def dehyphenate(record):
    """
        Removes hyphens of a record's make field, replacing them with spaces
    """
    record[MAKE_IDX] = record[MAKE_IDX].replace("-", " ")
    return record


if __name__ == "__main__":
    with beam.Pipeline() as pipeline:
        cars_data = (pipeline
            | "Read input" >> beam.io.ReadFromText('cars.csv')
            | "Filter out headers" >> beam.ParDo(FilterOutHeader('make'))
            | "Remove duplicates" >> beam.Distinct()
            | "Split by separator" >> beam.Map(lambda line: line.split(','))
            | "Display expensive" >> beam.Filter(lambda record: int(record[PRICE_IDX]) > 100000)
            | "De-hyphenate" >> beam.Map(dehyphenate)
            | "Join data" >> beam.Map(lambda record: ','.join(record))
            | "Output to file" >> beam.io.WriteToText('salida.txt'))
        
        print(cars_data)
