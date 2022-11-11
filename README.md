# Beam Example

A small sketchpad for Apache Beam pipelines with Python

It uses the [Belarus Used Cars Prices](https://www.kaggle.com/datasets/slavapasedko/belarus-used-cars-prices) Kaggle dataset to perform transform operations on its data and load it onto MongoDB Atlas

## Execution

Create a virtual environment and install the `apache_beam` and `dnspython` dependencies with PIP

Run the `main.py` script using

`python main.py --db_user=<database user> --db_pass=<database password> --db_host=<database host>`

Optionally, the input can be specified using the `--input=<path to input data>` argument