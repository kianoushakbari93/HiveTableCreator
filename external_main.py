"""generates the tables in Hive Metastore using Spark 3"""
import os.path
import sys
import datetime
import string
import random

from pyspark.sql import SparkSession
from pyspark.sql.types import (LongType, StructType, StructField, StringType,
                               IntegerType, ArrayType, TimestampType, MapType)

VOLUME_1000 = int(sys.argv[1])
VOLUME = VOLUME_1000 * 1000
PHONE_NUM = sys.argv[2].lower() == "true"
TIMESTAMP = sys.argv[3].lower() == "true"
ADDRESS_STRUCT = sys.argv[4].lower() == "true"
EXTERNAL_TABLE_STORAGE = str.lower(sys.argv[5])
DB_NAME_ITER = sys.argv[6]
EXTERNAL_PATH = sys.argv[7]

START_DATE = datetime.date(1980, 1, 1)
END_DATE = datetime.date(1990, 1, 1)
DATE_DIFF = (END_DATE - START_DATE).days
UPPER = string.ascii_uppercase
DIGITS = string.digits
spark = SparkSession.builder.getOrCreate()

structure = [
    StructField("first_name", StringType(), True),
    StructField("middle_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("birth_day", IntegerType(), True),
    StructField("birth_month", IntegerType(), True),
    StructField("birth_year", IntegerType(), True),
    StructField("age", IntegerType(), True)]

if PHONE_NUM:
    structure.append(StructField("phone_num", LongType(), True))

if TIMESTAMP and EXTERNAL_TABLE_STORAGE != "avro":
    structure.append(StructField("entry_creation", TimestampType(), True))

if ADDRESS_STRUCT and EXTERNAL_TABLE_STORAGE != "csv":
    structure.append(StructField("address", ArrayType(StructType([
        StructField("post_code", StringType(), True),
        StructField("street_address",
                    MapType(StringType(), IntegerType()),
                    True)]))))

schema = StructType(structure)

with open('./data/first-names.txt', encoding='utf-8') as fn_file:
    first_name = fn_file.read().split('\n')

with open('./data/middle-names.txt', encoding='utf-8') as mn_file:
    middle_name = mn_file.read().split('\n')

with open('./data/names.txt', encoding='utf-8') as ln_file:
    last_name = ln_file.read().split('\n')

with open('./data/streets.txt', encoding='utf-8') as st_file:
    streets = st_file.read().split('\n')


def generate_hdfs_path():
    """Generating HDFS path for both table and database"""

    valid_path = os.path.join(EXTERNAL_PATH, "")
    hdfs_path = 'hdfs://' + valid_path + DB_NAME_ITER + '.db/'
    return hdfs_path


def create_external_path(table_name):
    """Generating the external path for the external tables"""

    hdfs_path_string = generate_hdfs_path()
    path_option = {'path': hdfs_path_string + table_name}
    return path_option


def random_num_phone():
    """generates random 11 digit integer beginning with 44"""

    phone_num = '44'
    for i in range(9):
        phone_num += str(random.randrange(i, i + 2))
    return int(phone_num)


def get_birth_day():
    """
    generates random date (between global start and end dates)
    to obtain birthday,month,year and age
    """

    birthday = START_DATE + datetime.timedelta(random.randrange(DATE_DIFF))
    day = int(birthday.day)
    month = int(birthday.month)
    year = int(birthday.year)
    age = int((START_DATE + (datetime.date.today() - birthday)
               ).year - START_DATE.year)
    return day, month, year, age


def get_address_struc():
    """
    generates array contain a random post code
    and map (containing street name and house number)
    """

    postcode = random.choice(UPPER) + random.choice(UPPER) + random.choice(
        DIGITS) + random.choice(DIGITS) + random.choice(UPPER) + random.choice(UPPER)
    street = random.choice(streets)
    house_num = int(random.randrange(1, 101))
    return postcode, street, house_num


def get_entry(_):
    """constructs the person dictionary for the data frame and return it"""

    person = {}
    day, month, year, age = get_birth_day()

    person["first_name"] = random.choice(first_name)
    person["middle_name"] = random.choice(middle_name)
    person["last_name"] = random.choice(last_name)
    person["birth_day"] = day
    person["birth_month"] = month
    person["birth_year"] = year
    person["age"] = age

    if PHONE_NUM:
        phone_num = random_num_phone()
        person["phone_num"] = phone_num
    if TIMESTAMP and EXTERNAL_TABLE_STORAGE != "avro":
        person["entry_creation"] = datetime.datetime.now()
    if ADDRESS_STRUCT and EXTERNAL_TABLE_STORAGE != "csv":
        postcode, street, house_num = get_address_struc()
        address = [{"post_code": postcode,
                    "street_address": {street: house_num}}]
        person["address"] = address
    return person


def create_data_frame():
    """creates a dataframe of the people data"""

    itr_range = range(VOLUME)
    people = spark.sparkContext.parallelize(itr_range).map(
        get_entry)
    people_data_frame = spark.createDataFrame(people, schema)
    return people_data_frame


def ingest_from_df_basic(table_name, df):
    """initiates basic table creation and ingests data from data frame"""

    path_option = create_external_path(table_name)
    df.write.mode('append').format(EXTERNAL_TABLE_STORAGE) \
        .saveAsTable(table_name, **path_option)


def ingest_from_df_partition(table_name, partitions, df):
    """initiates partitioned table creation from the same data frame"""

    path_option = create_external_path(table_name)
    df.write.mode('append').format(EXTERNAL_TABLE_STORAGE).partitionBy(partitions) \
        .saveAsTable(table_name, **path_option)


def ingest_from_df_property(table_name, properties, df):
    """
    initiates table creation with
    table properties set and ingests
    data from data frame
    """

    path_option = create_external_path(table_name)
    path_prop = {**path_option, **properties}
    df.write.mode('append').format(EXTERNAL_TABLE_STORAGE) \
        .saveAsTable(table_name, **path_prop)


def column_remove_table(df):
    """creates table and drops middle_name column from it"""

    properties = {"my.key.id1": "2",
                  "my.key.id2": "5"}
    drop_column_df = df.drop("middle_name")
    ingest_from_df_property("column_remove_table", properties, drop_column_df)


def union_table_create(table_name, table1, table2):
    """constructs and writes union table creation statement to an external file"""

    df1 = spark.read.table(table1)
    df2 = spark.read.table(table2)
    df3 = df1.union(df2)
    path_option = create_external_path(table_name)
    df3.write.mode('append').format(EXTERNAL_TABLE_STORAGE) \
        .saveAsTable(table_name, **path_option)


def create_tables(df):
    """creates external tables using the data frame"""

    ingest_from_df_basic("initial_table", df)
    ingest_from_df_basic("initial_table_2", df)

    if PHONE_NUM:
        ingest_from_df_partition("bigint_partition_table", ["phone_num"], df)
    if TIMESTAMP and EXTERNAL_TABLE_STORAGE != "avro":
        ingest_from_df_partition(
            "timestamp_partition_table", ["entry_creation"], df)

    ingest_from_df_partition("multi_partition_table", [
        "birth_day", "birth_month", "birth_year"], df)

    ingest_from_df_property("properties_table", {
        "this.is.my.key": "14", "this.is.my.key2": "false"}, df)

    union_table_create("union_table",
                       "initial_table",
                       "initial_table_2")

    column_remove_table(df)

    spark.sql("show tables;").show()


if __name__ == '__main__':
    db_location = generate_hdfs_path()

    spark.sql("create database if not exists " +
              DB_NAME_ITER +
              " location " +
              "'" + db_location + "'")
    spark.sql("use " + DB_NAME_ITER)

    data_frame = create_data_frame()
    create_tables(data_frame)
    spark.stop()
