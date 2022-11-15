# Functions used in pipeline tranformations

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
