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


if __name__ == "__main__":
    with beam.Pipeline() as pipeline:
        cars_data = (pipeline 
            | "Read Input" >> beam.io.ReadFromText('cars.csv')
            | "Filter out headers" >> beam.ParDo(FilterOutHeader('make'))
            | "Split by Separator" >> beam.Map(lambda line: line.split(','))
            | "Display Expensive" >> beam.Filter(lambda record: int(record[2]) > 100000)
            | "Join data" >> beam.Map(lambda record: ','.join(record))
            | "Print" >> beam.io.WriteToText('salida.txt'))
        
        print(cars_data)
