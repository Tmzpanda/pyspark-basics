from sparksql.context import spark_session
from pyspark.sql.functions import col, concat_ws,lit
import json


def process():
    # extract
    df = spark_session \
        .read \
        .parquet("resources/employees")
    df.show(5)

    # transform
    with open("resources/etl_config.json", 'r') as config_file:
        config_dict = json.load(config_file)
    steps_per_floor = config_dict["steps_per_floor"]

    df_transformed = df.select(col("id"),
                               concat_ws(' ', col("first_name"), col("second_name")).alias("name"),
                               (col("floor") * lit(steps_per_floor)).alias("steps_to_desk"))
    df_transformed.show(5)

    # load
    df_transformed\
        .write\
        .csv("resources/loaded_data", mode="overwrite", header=True)


if __name__ == '__main__':
    process()

