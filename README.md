# Beam Example

A small sketchpad for Apache Beam pipelines with Python to be used as Flex template for Google Cloud Dataflow

It uses the [Belarus Used Cars Prices](https://www.kaggle.com/datasets/slavapasedko/belarus-used-cars-prices) Kaggle dataset to perform transform operations on its data and load it onto MongoDB Atlas

## Execution

Create a virtual environment and install the `apache_beam` and `dnspython` dependencies with PIP

To run locally, execute the `main.py` script using

`python main.py --db_user=<database user> --db_pass=<database password> --db_host=<database host>`

Optionally, the specify the input file path using the `--input=<path to input data>` argument

## Deployment

To deploy on GCP execute the `deploy.sh` scrpit. It requires the following variables:
- `GCP_REGION`
- `GCP_PROJECT`
- `GCP_ARTIFACT_REPO`
- `GCP_BUCKET`