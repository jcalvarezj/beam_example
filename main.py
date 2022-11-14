# Entry point

import argparse, sys
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.mongodbio import WriteToMongoDB, ReadFromMongoDB

from pipeline_fns.pipeline_fns import *
from do_fns.do_fns import FilterOutHeader
from composite_transforms.transforms import BonusJoinByType

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

    db_name = "carsdb"

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

        print("Obtaining MongoDB bonus prices table data")
        bonus_data = (pipeline
            | "Read bonus prices" >> ReadFromMongoDB(uri=db_uri, 
                                                     db=db_name,
                                                     coll="bonus_prices",
                                                     filter={"type": "drive_unit"},
                                                     bucket_auto=True))

        print("Obtaining clean data")
        cars_data = (pipeline
            | "Read input" >> ReadFromText(data_input)
            | "Filter out headers" >> beam.ParDo(FilterOutHeader('make'))
            | "Remove duplicates" >> beam.Distinct()
            | "Split by separator" >> beam.Map(lambda line: line.split(','))
            | "Convert to dict" >> beam.Map(convert_to_dict, col_indexes)
            | "Filter expensive" >> beam.Filter(lambda record: int(record["price"]) >= 100000) ####### 7000
            | "Clean volume data" >> beam.Map(standardize_empty_numeric_field, ["volume"])
            | "De-hyphenate" >> beam.Map(dehyphenate))
        
        print("Enriching valid cars data")
        valid_cars = (cars_data
             | "Filter cars with drive-unit" >> beam.Filter(lambda record: record["drive_unit"] != ""))

        enriched_data = (valid_cars
            | "Join valid car data and bonus price" >> BonusJoinByType(bonus_data)
            | beam.Map(print)
        )
        
        print("Sinking cars with drive-unit")
        sink_valid = (valid_cars
            | "Sink valid cars to MongoDB" >> WriteToMongoDB(uri=db_uri, db=db_name, coll="valid_cars"))

        print(sink_valid)

        print("Sinking cars without drive-unit")
        sink_invalid = (cars_data
            | "Filter cars without drive-unit" >> beam.Filter(lambda record: record["drive_unit"] == "")
            | "Sink invalid cars to MongoDB" >> WriteToMongoDB(uri=db_uri, db=db_name, coll="invalid_cars"))
        
        print(sink_invalid)

    print("Finished")

if __name__ == "__main__":
    main()
