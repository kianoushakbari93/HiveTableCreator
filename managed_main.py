"""
generates the sql files required to create,
populate and perform pre-cursor steps for the scripts tables
"""
import sys
import datetime
import string
import random

VOLUME_1000 = int(sys.argv[1])
VOLUME = VOLUME_1000 * 1000
PHONE_NUM = sys.argv[2].lower() == "true"
TIMESTAMP = sys.argv[3].lower() == "true"
ADDRESS_STRUCT = sys.argv[4].lower() == "true"
EXTERNAL = sys.argv[5].lower() == "true"
MANAGED_TABLE_STORAGE = " ".join(sys.argv[6:])

START_DATE = datetime.date(1980, 1, 1)
END_DATE = datetime.date(1990, 1, 1)
DATE_DIFF = (END_DATE - START_DATE).days
UPPER = string.ascii_uppercase
DIGITS = string.digits
NO_COLUMN_DROP = ["orc", "avro"]
PHONE_NUMS_CREATED = set()

FIELDS_DICT = {"first_name": "string",
               "middle_name": "string",
               "last_name": "string",
               "birth_day": "int",
               "birth_month": "int",
               "birth_year": "int",
               "age": "int"}

if PHONE_NUM:
    FIELDS_DICT["phone_num"] = "bigint"
if TIMESTAMP:
    FIELDS_DICT["entry_creation"] = "timestamp"
if ADDRESS_STRUCT:
    FIELDS_DICT["address"] = "array <struct<post_code:string,street_address:map<string,int>>>"

CREATION_STRING = ", ".join(
    [f"{key} {value}" for key, value in FIELDS_DICT.items()])
INSERTION_STRING = ", ".join(FIELDS_DICT.keys())

with open('./data/first-names.txt', encoding='utf-8') as fn_file:
    first_name = fn_file.read().split('\n')

with open('./data/middle-names.txt', encoding='utf-8') as mn_file:
    middle_name = mn_file.read().split('\n')

with open('./data/names.txt', encoding='utf-8') as ln_file:
    last_name = ln_file.read().split('\n')

with open('./data/streets.txt', encoding='utf-8') as st_file:
    streets = st_file.read().split('\n')


def random_num_phone():
    """generates random 11 digit integer beginning with 44"""
    phone_num = '44'
    for i in range(9):
        phone_num += str(random.randrange(i, i + 2))
    PHONE_NUMS_CREATED.add(phone_num)
    return phone_num


def get_birth_day():
    """
    generates random date (between global start and end dates)
    to obtain birthday, month, year and age
    """
    birthday = START_DATE + datetime.timedelta(random.randrange(DATE_DIFF))
    day = str(birthday.day)
    month = str(birthday.month)
    year = str(birthday.year)
    age = str((START_DATE + (datetime.date.today() - birthday)
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
    house_num = str(random.randrange(1, 101))
    return postcode, street, house_num


def get_entry():
    """
    constructs the list of values used in an individual entry
    within a bulk insert statement
    """
    birth_day_info = get_birth_day()
    postcode, street, house_num = get_address_struc()
    fields = ["'" + random.choice(first_name) + "'", "'" + random.choice(
        middle_name) + "'", "'" + random.choice(last_name) + "'", *birth_day_info]
    if PHONE_NUM:
        fields.append(random_num_phone())
    if TIMESTAMP:
        fields.append("CURRENT_TIMESTAMP")
    if ADDRESS_STRUCT:
        fields.append("array(named_struct('post_code', '" + postcode +
                      "', 'street_address', map('" + street + "', " + house_num + ")))")
    entry_string = ",".join(fields)
    return entry_string


def write_ingest_file(file):
    """constructs and writes bulk data ingest statements to an external file"""
    with open(file, "a", encoding='utf-8') as data_file:
        for _ in range(VOLUME_1000):
            data_file.write("insert into initial_table values ")
            entry_arr = ["(" + get_entry() + ")" for _ in range(1000)]
            data_file.write(",".join(entry_arr))
            data_file.write(";")


def basic_table_create(writer, name):
    """constructs and writes basic table creation statement to an external file"""
    writer.write("create ")
    if EXTERNAL:
        writer.write("external ")
    writer.write("table " + name + "(" + CREATION_STRING + ") " +
                 MANAGED_TABLE_STORAGE)
    if EXTERNAL:
        writer.write(" location '/user/hive/${hiveconf:dbname}/" + name + "'")
    writer.write(";\n\n")

    insert_statement = "insert into " + name + " select " + \
                       INSERTION_STRING + " from initial_table"
    return insert_statement


def partition_table_create(writer, name, partition_arr):
    """constructs and writes partitioned table creation statement to an external file"""
    partitioned = {}
    non_partitioned = {}

    for key, val in FIELDS_DICT.items():
        if key in partition_arr:
            partitioned[key] = val
        else:
            non_partitioned[key] = val

    np_create = ", ".join(
        [f"{key} {value}" for key, value in non_partitioned.items()])
    p_create = ", ".join(
        [f"{key} {value}" for key, value in partitioned.items()])

    np_insert = ", ".join(non_partitioned.keys())
    p_insert = ", ".join(partitioned.keys())

    writer.write("create ")
    if EXTERNAL:
        writer.write("external ")
    writer.write("table " + name + "(" + np_create +
                 ") partitioned by (" + p_create + ") " + MANAGED_TABLE_STORAGE)
    if EXTERNAL:
        writer.write(" location '/user/hive/${hiveconf:dbname}/" + name + "'")
    writer.write(";\n\n")

    p_as_string = ""
    for key in partitioned.keys():
        p_as_string += "," + key + " as " + key

    insert_statement = "insert into " + name + \
                       " partition (" + p_insert + ") select " + np_insert + \
                       p_as_string + " from initial_table"
    return insert_statement


def property_table_create(writer, name, property_arr):
    """
    constructs and writes table creation statement with table properties set to an external file
    """
    writer.write("create ")
    if EXTERNAL:
        writer.write("external ")
    writer.write("table " + name + "(" + CREATION_STRING + ") " +
                 MANAGED_TABLE_STORAGE)
    if EXTERNAL:
        writer.write(" location '/user/hive/${hiveconf:dbname}/" + name + "'")
    writer.write(" TBLPROPERTIES(" + ",".join(property_arr) + ");\n\n")

    insert_statement = "insert into " + name + " select " + \
                       INSERTION_STRING + " from initial_table"
    return insert_statement


def union_table_create(writer, name, table1, table2):
    """constructs and writes union table creation statement to an external file"""
    writer.write("create ")
    if EXTERNAL:
        writer.write("external ")
    writer.write("table " + name + " " + MANAGED_TABLE_STORAGE)
    if EXTERNAL:
        writer.write(" location '/user/hive/${hiveconf:dbname}/" + name + "'")
    writer.write(" as select * from " + table1 +
                 " union all select * from " + table2 + ";\n\n")


def staggered_ingest_birth_day(table_file, insert_statement):
    """
    constructs and writes table insert statements broken up by birth_day value to an external file
    """
    for i in range(1, 32):
        table_file.write(insert_statement + " where birth_day=" + str(i) + ";")
    table_file.write("\n")


def staggered_ingest_phone_num(table_file, insert_statement):
    """
    constructs and writes table insert statements broken up by phone_num value to an external file
    """
    for num in PHONE_NUMS_CREATED:
        table_file.write(insert_statement + " where phone_num=" + num + ";")
    table_file.write("\n")


def write_table_file(file):
    """
    construct and writes sql used to create
    and populate all tables within an external file
    """
    with open(file, "a", encoding='utf-8') as table_file:
        table_file.write(
            """set dbname;\n
            set hive.exec.dynamic.partition.mode=nonstrict;\n
            set hive.exec.max.dynamic.partitions.pernode=50000;\n
            set hive.exec.max.dynamic.partitions=50000;\n
            create database ${hiveconf:dbname};\n
            use ${hiveconf:dbname};\n\n"""
        )

    basic_table_create(table_file, "initial_table")
    table_file.write("!run /tmp/HiveDataIngest.sql\n")

    staggered_ingest_birth_day(
        table_file, basic_table_create(table_file, "initial_table_2"))

    if not any(table_format in MANAGED_TABLE_STORAGE.lower() for table_format in NO_COLUMN_DROP):
        staggered_ingest_birth_day(
            table_file, basic_table_create(table_file, "column_drop_table"))
        column_drop_list = (CREATION_STRING.split(", "))[:-1]
        table_file.write(
            "alter table column_drop_table replace "
            + "columns (" + ",".join(column_drop_list) + ");\n\n")

    truncate_insert = basic_table_create(table_file, "truncate_insert_table")
    staggered_ingest_birth_day(table_file, truncate_insert)
    table_file.write(
        "alter table truncate_insert_table SET TBLPROPERTIES ('external.table.purge'='true');")
    table_file.write("truncate table truncate_insert_table;")
    staggered_ingest_birth_day(table_file, truncate_insert)

    staggered_ingest_birth_day(
        table_file,
        partition_table_create(table_file, "multi_partition_table", [
            "birth_day", "birth_month", "birth_year", "age"])
    )

    if PHONE_NUM:
        staggered_ingest_phone_num(
            table_file,
            partition_table_create(
                table_file, "bigint_partition_table", ["phone_num"])
        )

    if TIMESTAMP:
        staggered_ingest_birth_day(
            table_file,
            partition_table_create(
                table_file, "timestamp_partition_table", ["entry_creation"])
        )

    staggered_ingest_birth_day(
        table_file,
        property_table_create(table_file, "properties_table", [
            "'this.is.my.key' = '14'", "'this.is.my.key2' = 'false'"])
    )

    union_table_create(table_file, "union_table",
                       "initial_table", "initial_table_2")

    table_file.write("show tables;")

    table_file.close()


if __name__ == '__main__':
    write_ingest_file("/tmp/HiveDataIngest.sql")
    write_table_file("/tmp/CreateTables.sql")
