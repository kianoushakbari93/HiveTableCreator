**Hive Table Generator**

**Overview**

These scripts are designed to create medium-to-large scale tables within Hive (up to 100,000 entries recommended). They
are highly parameterised with users able to define whether the tables built are managed/external, contain specific data
types as well as how many entries they contain and what metadata format is used to store them.

**Tables created**

This set of scripts creates the following tables:

- **initial_table:** basic table created from ingest data
- **initial_table_2:** duplicate of original ingest table used in the creation of union_table
- **column_drop_table:** identical table to initial_table with a column dropped from it
- **truncate_insert_table:** identical table to initial_table with the ingested data truncated before being inserted
  again
- **multi_partition_table:** table partitioned on birthday, month, year and age
- **bigint_partition_table:** table partitioned on bigint phone number values
- **timestamp_partition_table:** table partitioned on timestamp entry creation values
- **properties_table:** table with arbitrary table properties set
- **union_table:** union of initial_table and initial_table_2

**Tables created by this script contain the following fields:**

- **first_name:** string
- **middle_name:** string
- **last_name:** string
- **birth_day:** int
- **birth_month:** int
- **birth_year:** int
- **age:** int
- **phone_num:** bigint
- **entry_creation:** timestamp
- **address:** array<struct<post_code:string,street_address:map<string,int>>>

**PLEASE NOTE that all external tables created by these scripts store their data within /user/hive/{database
name}/{table name} on the HDFS.**

**Default Configurable parameters**

**Available Runtime Arguments:**

--help: More info on script and arguments available

--hp: Hive principal

--hk: Hive keytab

--db: Database name

--vol: Volume (counted per 1000, 100 -> 100,000)

--bint: Include bigint (true/false)

--tstmp: Include timestamp (true/false)

--struct: Include struct (true/false)

--ext: External tables (true/false)

--esto: Metadata storage type only for external table (e.g. parquet, orc, csv, etc.)

--msto: Metadata storage type only for managed table (e.g. stored as parquet)

--iter: How many times should scripts run

--hver: Hive version on cluster. hint: "hive --version" - (e.g. 3.1.3)

--huri: Hive metastore thrift URI. (e.g. thrift://HMS-SERVER.com:9083)

--xpath: Path on HDFS to store the tables

--kerb: Boolean for kerberos on cluster (true/false)

**In start.sh:**

**Default Configurable Parameters**

- HIVE_PRINCIPAL: Hive kerberos principal
- HIVE_KEYTAB: Location of Hive kerberos keytab
- DB_NAME: Name of database created by scripts (must be unique each time the script is run)
- VOLUME_1000: Number of entries within each table created, measured per 1000 (i.e. 100 -> 100,000 entries per table)
- BIGINT: Boolean to determine if the user wants the phone number bigint field to be used and bigint_partition_table to
  be created
- TIMESTAMP: Boolean to determine if the user wants the entry creation timestamp field to be used and
  timestamp_partition_table to be created (Not supported in AVRO tables)
- STRUCT: Boolean to determine if the user wants the address array field to be used (Not supported in CSV tables)
- EXTERNAL: Boolean to determine whether the tables created are external or managed
- ITERATIONS: How many time should the script run (i.e. ITERATIONS=2 will produce sample_db and sample_db_2)

**Managed Table Configurable Parameters**

- MANAGED_TABLE_STORAGE: Metadata storage type only for managed table (e.g. stored as parquet)

**External Table Configurable Parameters (Required)**

- SPARK_DL_LINK: Link to download Apache Spark. Apache keep releasing the new versions and replacing them with the old
  ones. This means every time they release a new branch, the old link will become invalid. You can find latest release
  download link here https://spark.apache.org/downloads.html
- HIVE_METASTORE_VERSION: The version of Hive on cluster. HINT: Run the "hive --version" on any VM
- HIVE_METASTORE_URIS: The thrift URI for HMS(Hive metastore). Search hive.metastore.uris in the hive-site.xml ->
  Usually HMS thrift is on vm0 on port 9083. Example: thrift://HMS-SERVER.com:9083
- EXTERNAL_PATH: Path on HDFS that the database and table going to be stored and user Hive can access to it (e.g.
  /user/hive/)
- EXTERNAL_TABLE_STORAGE: Metadata storage type only for external table (e.g. parquet, orc, csv, avro or json)
- EXTERNAL_ONLY_KERBERIZED: Set to True if cluster that is running the script is Kerberized. Otherwise, set to False

**Default values:**

HIVE_PRINCIPAL = "hive-principal@REALM.HADOOP"
HIVE_KEYTAB = "Path-to-keytab"
DB_NAME = "sample_db"
VOLUME_1000 = 100
BIGINT = True
TIMESTAMP = True
STRUCT = True
EXTERNAL = True
TABLE_STORAGE = "stored as parquet"
ITERATIONS = 1

**How to run:**

**Become sudo (root) user**

- su --

**Install Python3**

- e.g. on CentOS 7: yum install python3

**Make start script executable**

- chmod +x start.sh

**Run start script**

- ./start.sh [--hp|--hk|--db|--vol|--bint|--tstmp|--struct|--ext|--msto/esto|--iter|--hver|--huri|--xpath|--kerb]

**(Optional) Run start script in background without exit**

- nohup ./start.sh [--hp|--hk|--db|--vol|--bint|--tstmp|--struct|--ext|--msto/esto|--iter|--hver|--huri|--xpath|--kerb]

**Points to note**

- It takes approximately 20 minutes to run these scripts with 100,000 entry external tables created.

- It takes approximately 45 minutes to run these scripts with 100,000 entry managed tables created.

- It should NOT be necessary to tune your cluster (Devcon) to run these scripts. They have been designed to ingest data
  in small subsections to avoid the need for a large volume of compute.

- When using Orc or Avro, column_drop_table will not be created as the 'replace columns' operation is not supported by
  these formats.
