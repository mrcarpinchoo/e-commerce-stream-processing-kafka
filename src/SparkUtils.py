import findspark
findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, isnull, when
from pyspark.sql.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    DateType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    ShortType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)


class SparkUtils:

    def __init__(self, app_name: str, master_url: str = None, jars: str = None, packages: str = None):
        builder = SparkSession.builder.appName(app_name)

        if master_url:
            builder = builder.master(master_url)
        # end if

        if jars:
            builder = builder.config("spark.jars", jars)
        # end if

        if packages:
            builder = builder.config("spark.jars.packages", packages)
        # end if

        builder = builder.config("spark.sql.adaptive.enabled", "false")

        self.spark = builder.getOrCreate()
        self.spark.conf.set("spark.sql.shuffle.partitions", "2")
    # end def

    @staticmethod
    def generate_schema(fields: list[tuple[str, str]]) -> StructType:
        """Build a StructType schema from a list of (field, type) tuples.

        Supported type strings:
            string, int, long, short, double, float, boolean,
            date, timestamp, binary, array_int, array_string
        """
        types_map = {
            "string":       StringType(),
            "int":          IntegerType(),
            "long":         LongType(),
            "short":        ShortType(),
            "double":       DoubleType(),
            "float":        FloatType(),
            "boolean":      BooleanType(),
            "date":         DateType(),
            "timestamp":    TimestampType(),
            "binary":       BinaryType(),
            "array_int":    ArrayType(IntegerType()),
            "array_string": ArrayType(StringType()),
        }

        return StructType([
            StructField(field, types_map[type_], nullable=True)
            for field, type_ in fields
        ])
    # end def

    @staticmethod
    def count_nulls(df):
        return df.select([count(when(isnull(col(c)), c)).alias(c) for c in df.columns])
    # end def

# end class
