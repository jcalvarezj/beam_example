# Entry point

import argparse, sys, time
import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.io import ReadFromText, WriteToBigQuery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.mongodbio import WriteToMongoDB, ReadFromMongoDB

from beam_example.pipeline_fns.pipeline_fns import *
from beam_example.do_fns.do_fns import FilterOutHeader
from beam_example.composite_transforms.transforms import BonusJoinByType

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
        parser.add_value_provider_argument('--gcp_project_id')
        parser.add_value_provider_argument('--gcp_bucket_id')


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
    parser.add_argument('--input', default="beam_example/cars.csv")
    parser.add_argument('--gcp_project_id', required=True)
    parser.add_argument('--gcp_bucket_id', required=True)
    args, pipeline_args = parser.parse_known_args(sys.argv)

    pipeline_options = PipelineOptions(pipeline_args)
    custom_options = pipeline_options.view_as(CustomPipelineOptions)

    with beam.Pipeline(options=custom_options) as pipeline:
        data_input = args.input
        db_uri = f"mongodb+srv://{args.db_user}:{args.db_pass}@{args.db_host}"

        bq_table_specs = bigquery.TableReference(projectId=args.gcp_project_id,
                                                 datasetId="cars_data",
                                                 tableId="car_pricing")
        bq_table_schema = {
            "fields": [
                {"name": "make", "type": "STRING", "mode": "REQUIRED"},
                {"name": "model", "type": "STRING", "mode": "REQUIRED"},
                {"name": "price", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "year", "type": "INTEGER", "mode": "REQUIRED"},
                {"name": "condition", "type": "STRING", "mode": "REQUIRED"},
                {"name": "mileage", "type": "NUMERIC", "mode": "REQUIRED"},
                {"name": "fuel_type", "type": "STRING", "mode": "REQUIRED"},
                {"name": "volume", "type": "STRING", "mode": "NULLABLE"},
                {"name": "transmission", "type": "STRING", "mode": "REQUIRED"},
                {"name": "drive_unit", "type": "STRING", "mode": "REQUIRED"},
                {"name": "bonus_type", "type": "STRING", "mode": "REQUIRED"},
                {"name": "bonus_price", "type": "INTEGER", "mode": "REQUIRED"}
            ]
        }
        # bq_table_schema = "SCHEMA_AUTODETECT"
        bq_temp_location = "gs://" + args.gcp_bucket_id + "/tmp"

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
            | "Filter expensive" >> beam.Filter(lambda record: record["price"] >= 7000 and record["volume"] != None)
            | "De-hyphenate" >> beam.Map(dehyphenate))
        
        print("Enriching valid cars data")
        valid_cars = (cars_data
             | "Filter cars with drive-unit" >> beam.Filter(lambda record: record["drive_unit"] != ""))

        enriched_data = (valid_cars
            | "Join valid car data and bonus price" >> BonusJoinByType(bonus_data)
            | "Remove unwanted columns" >> beam.Map(
                lambda record: 
                    {key: record[key] for key in record if key not in ["color", "segment", "_id"]}
            )
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

        print("Sinking joined data")
        sink_enriched = (enriched_data
            | "Sink join-enriched cars to BigQuery" >> WriteToBigQuery(bq_table_specs,
                                                                       schema=bq_table_schema,
                                                                       custom_gcs_temp_location=bq_temp_location))
        
        print(sink_enriched)

    print("Finished")

if __name__ == "__main__":
    print("Starting ETL pipeline")
    start = time.time()
    try:
        main()
    except Exception as e:
        print("An error occurred when processing the pipeline")
        print(e)
    finally:
        end = time.time()
        print(f"Pipeline execution lasted {end - start} seconds")