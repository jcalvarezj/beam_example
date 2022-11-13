import argparse, sys, os
import apache_beam as beam
from apache_beam.io.mongodbio import WriteToMongoDB
from apache_beam.options.pipeline_options import PipelineOptions


class CustomPipelineOptions(PipelineOptions):
    """
    Runtime arguments used to include database connection data
    """
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument('--input')
        parser.add_value_provider_argument('--db_user')
        parser.add_value_provider_argument('--db_pass')
        parser.add_value_provider_argument('--db_host')


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
    record["make"] = record["make"].replace("-", " ")
    return record


def convert_to_dict(line_fields, col_indexes):
    """
        Transforms the line_fields element to a dictionary
    """
    return {
        "make": line_fields[col_indexes["make"]],
        "model": line_fields[col_indexes["model"]],
        "price": float(line_fields[col_indexes["price"]]),
        "year": int(line_fields[col_indexes["year"]]),
        "condition": line_fields[col_indexes["condition"]],
        "mileage": float(line_fields[col_indexes["mileage"]]),
        "fuel_type": line_fields[col_indexes["fuel_type"]],
        "volume": line_fields[col_indexes["volume"]],
        "color": line_fields[col_indexes["color"]],
        "transmission": line_fields[col_indexes["transmission"]],
        "drive_unit": line_fields[col_indexes["drive_unit"]],
        "segment": line_fields[col_indexes["segment"]]
    }


def standardize_empty_numeric_field(record, field_indexes):
    """
        Inserts "None" to the field if it's empty or has a "NaN" value
    """
    for i in field_indexes:
        record[i] = float(record[i]) if record[i] != "" and record[i].lower() != "nan" else None
    return record


def main():
    """
        Entry point of the Beam application
    """
    col_indexes = {
        "make": 0,
        "model": 1,
        "price": 2,
        "year": 3,
        "condition": 4,
        "mileage": 5,
        "fuel_type": 6,
        "volume": 7,
        "color": 8,
        "transmission": 9,
        "drive_unit": 10,
        "segment": 11
    }

    parser = argparse.ArgumentParser()
    parser.add_argument('--db_user', required=True)
    parser.add_argument('--db_pass', required=True)
    parser.add_argument('--db_host', required=True)
    parser.add_argument('--input', default='cars.csv')
    args, pipeline_args = parser.parse_known_args(sys.argv)

    pipeline_options = PipelineOptions(pipeline_args)
    custom_options = pipeline_options.view_as(CustomPipelineOptions)

    with beam.Pipeline(options=custom_options) as pipeline:
        data_input = args.input
        db_uri = f"mongodb+srv://{args.db_user}:{args.db_pass}@{args.db_host}"

        print("Obtaining clean data")
        cars_data = (pipeline
            | "Read input" >> beam.io.ReadFromText(data_input)
            | "Filter out headers" >> beam.ParDo(FilterOutHeader('make'))
            | "Remove duplicates" >> beam.Distinct()
            | "Split by separator" >> beam.Map(lambda line: line.split(','))
            | "Convert to dict" >> beam.Map(convert_to_dict, col_indexes)
            | "Filter expensive" >> beam.Filter(lambda record: int(record["price"]) >= 7000)
            | "Clean volume data" >> beam.Map(standardize_empty_numeric_field, ["volume"])
            | "De-hyphenate" >> beam.Map(dehyphenate))

        print("Sinking cars with drive-unit ")
        sink_valid = (cars_data
            | "Filter cars with drive-unit" >> beam.Filter(lambda record: record["drive_unit"] != "")
            | "Sink valid cars to MongoDB" >> WriteToMongoDB(uri=db_uri, db='carsdb', coll='valid_cars'))

        print(sink_valid)

        print("Sinking cars without drive-unit")
        sink_invalid = (cars_data
            | "Filter cars without drive-unit" >> beam.Filter(lambda record: record["drive_unit"] == "")
            | "Sink invalid cars to MongoDB" >> WriteToMongoDB(uri=db_uri, db='carsdb', coll='invalid_cars'))
        
        print(sink_invalid)

    print("Finished")

if __name__ == "__main__":
    main()
