# schema.py

from pyspark.sql.types import *

def get_schema():

    return StructType([
    StructField("account_no", StringType(), True),
    StructField("transaction_date", StringType(), True),
    StructField("transaction_details", StringType(), True),
    StructField("chqno", StringType(), True),
    StructField("value_date", StringType(), True),
    StructField("withdrawal_amt", StringType(), True),
    StructField("deposit_amt", StringType(), True),
    StructField("balance_amt", StringType(), True),
    ])
    