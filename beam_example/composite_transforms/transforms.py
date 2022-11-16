# User-defined composite transforms

import apache_beam as beam


class BonusJoinByType(beam.PTransform):
    """
    PTransform that joins relationally valid car data and bonus price data
    """
    def __init__(self, bonus_prices_coll):
        self.bonus_prices_coll = bonus_prices_coll
    
    def _convert_to_common_key_structure(self, record, key):
        """
        Returns a tuple of a record's common join key's value (bonus type) mapped to the record's data
        """
        return record[key], record

    def _unnest_cogroup(self, element):
        """
        Returns the CoGroup element's data formatted as a list of dicts to be flattened
        """
        car_data = element[1][0]
        bonus_data = element[1][1][0]
        join_data = []
        for car in car_data:
            current = car
            current["bonus_type"] = "drive_unit"
            current["bonus_price"] = bonus_data["price"]
            join_data.append(current)
        return join_data

    def expand(self, valid_cars_coll):
        """
        Returns the pipeline operations of this PTransform
        """
        valid_cars_map = (valid_cars_coll
            | "Bonus Join - Map Valid" >> beam.Map(self._convert_to_common_key_structure, "drive_unit"))
        bonus_data_map = (self.bonus_prices_coll
            | "Bonus Join - Map Bonus" >> beam.Map(self._convert_to_common_key_structure, "value"))
        pipeline_result = ((valid_cars_map, bonus_data_map)
            | "Bonus Join - CoGroup" >> beam.CoGroupByKey()
            | "Unnest CoGroup" >> beam.FlatMap(self._unnest_cogroup))

        return pipeline_result
