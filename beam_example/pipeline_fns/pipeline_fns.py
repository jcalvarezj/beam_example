# Functions used in pipeline tranformations

def convert_to_dict(line_fields, col_indexes):
    """
        Transforms the line_fields element to a dictionary
    """
    make = line_fields[col_indexes["make"]]
    volume = line_fields[col_indexes["volume"]]
    return {
        "make": make.replace("-", " "),
        "model": line_fields[col_indexes["model"]],
        "price": int(line_fields[col_indexes["price"]]),
        "year": int(line_fields[col_indexes["year"]]),
        "condition": line_fields[col_indexes["condition"]],
        "mileage": float(line_fields[col_indexes["mileage"]]),
        "fuel_type": line_fields[col_indexes["fuel_type"]],
        "volume": float(volume) if volume != "" and volume != "nan" else None,
        "color": line_fields[col_indexes["color"]],
        "transmission": line_fields[col_indexes["transmission"]],
        "drive_unit": line_fields[col_indexes["drive_unit"]],
        "segment": line_fields[col_indexes["segment"]]
    }
