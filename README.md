# Beam Example

A small sketchpad for Apache Beam pipelines with Python to be used as Flex template for Google Cloud Dataflow

It uses the [Belarus Used Cars Prices](https://www.kaggle.com/datasets/slavapasedko/belarus-used-cars-prices) Kaggle dataset to perform transform operations on its data and load it onto MongoDB Atlas and Google BigQuery (using an already existing table on MongoDB to enrich clean and valid data)

An IPython Notebook is included ([cars_data_exploration.ipynb](cars_data_exploration.ipynb)), which displays the data exploration drafts of the CSV dataset to have an idea of the its contents and treatment

## Execution

Create a virtual environment and install the `apache_beam` and `dnspython` dependencies with PIP

To run locally, execute the [main.py](beam_example/main.py) script using

`python beam_example/main.py <arguments>`

The required arguments are

- `--db_user=<database user>`
- `--db_pass=<database password>`
- `--db_host=<database host>`
- `--gcp_project_id=<project id>`
- `--gcp_bucket_id=<bucket id>`

The input file path can be specified using the optional `--input=<path to input data>` argument

## Deployment

To deploy on GCP execute the `deploy.sh` script. It requires the following variables:
- `GCP_REGION`
- `GCP_PROJECT`
- `GCP_ARTIFACT_REPO`
- `GCP_BUCKET`