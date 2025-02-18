#!/bin/bash

###DEFAULT CONFIGURED PARAMS###
HIVE_PRINCIPAL="hive-principal@REALM.HADOOP"
HIVE_KEYTAB="Path-to-keytab"
DB_NAME="sample_db"
VOLUME_1000=100
BIGINT=True
TIMESTAMP=True
STRUCT=True
EXTERNAL=True
ITERATIONS=1
###############################

###MANAGED TABLE CONFIGURED PARAMS###
MANAGED_TABLE_STORAGE="stored as parquet"
#MANAGED_TABLE_STORAGE="stored as orc"
#MANAGED_TABLE_STORAGE="stored as avro"
#MANAGED_TABLE_STORAGE="row format delimited fields terminated by ',' lines terminated by '\n' stored as textfile"
###############################

###EXTERNAL TABLE CONFIGURED PARAMS###
SPARK_DL_LINK="https://dlcdn.apache.org/spark/spark-3.5.2/spark-3.5.2-bin-hadoop3-scala2.13.tgz"
HIVE_METASTORE_VERSION="X.X.X"
HIVE_METASTORE_URIS="thrift://HMS-SERVER.com:9083"
EXTERNAL_PATH="/user/hive/"
#EXTERNAL_TABLE_STORAGE=parquet
EXTERNAL_TABLE_STORAGE=orc
#EXTERNAL_TABLE_STORAGE=csv
#EXTERNAL_TABLE_STORAGE=json
#EXTERNAL_TABLE_STORAGE=avro
EXTERNAL_ONLY_KERBERIZED=True
###############################

help() {
  echo
  echo "Scripts to create sample Hive tables"
  echo
  echo "Syntax: ./start.sh [--hp|--hk|--db|--vol|--bint|--tstmp|--struct|--ext|--esto|--msto]"
  echo
  echo "Options:"
  echo
  echo "hp        Hive kerberos principal"
  echo
  echo "hk        Location of Hive kerberos keytab"
  echo
  echo "db        Name of database created by scripts"
  echo "          (must be unique each time the script is run)"
  echo
  echo "vol       Number of entries within each table created,"
  echo "          measured per 1000 (i.e. 100 -> 100,000 entries per table)"
  echo
  echo "bint      boolean to determine if the user wants the phone number"
  echo "          bigint field to be used and bigint_partition_table to be created"
  echo
  echo "tstmp     boolean to determine if the user wants the entry creation"
  echo "          timestamp field to be used and timestamp_partition_table to be created"
  echo
  echo "struct    boolean to determine if the user wants the address array field to be used"
  echo
  echo "ext       boolean to determined wether the tables created are external or managed"
  echo
  echo "esto      how the external tables created will be stored (metadata format),"
  echo "          expects the table type segment (i.e. parquet, orc, avro, etc.)"
  echo
  echo "msto      how the managed external tables created will be stored (metadata format),"
  echo "          expects a full sql segment (i.e. stored by parquet)"
  echo
  echo "iter      how many time should the script run"
  echo "          (i.e. --iter 2 will produce sample_db and sample_db_2)"
  echo
  echo "hver      version of Hive on cluster. hint: run this command -> hive --version"
  echo "          expects Hive version in X.X.X format (i.e. 3.1.3, 3.1.0)"
  echo
  echo "huri      The thrift URI for HMS(Hive metastore). Search hive.metastore.uris in the hive-site.xml"
  echo "          expects HMS thrift on vm0 on port 9083. (i.e. thrift://HMS-SERVER.com:9083)"
  echo
  echo "xpath     Path on HDFS that the database and table going to be stored and user Hive can access to it"
  echo "          expects a path on HDFS that is owned by Hive user (i.e /user/hive/)"
  echo
  echo "kerb      bolean to determine if the cluster is Kerberos enabled or not"
  echo "          True: if Kerberized, False: if Unkerberized"
  echo
  echo "================================="
  echo "         CONFIG VALUES"
  echo "================================="
  echo
  if [ "${EXTERNAL_ONLY_KERBERIZED,,}" = true ]; then
    echo "HIVE_PRINCIPAL = $HIVE_PRINCIPAL"
    echo "HIVE_KEYTAB = $HIVE_KEYTAB"
  fi
  echo "DB_NAME = $DB_NAME"
  echo "VOLUME_1000 = $VOLUME_1000"
  echo "BIGINT = $BIGINT"
  echo "TIMESTAMP = $TIMESTAMP"
  echo "STRUCT = $STRUCT"
  echo "EXTERNAL = $EXTERNAL"
  if [ ${EXTERNAL,,} = true ]; then
    echo "TABLE_STORAGE = $EXTERNAL_TABLE_STORAGE"
    echo "HIVE_VERSION = $HIVE_METASTORE_VERSION"
    echo "HMS_URIs= $HIVE_METASTORE_URIS"
    echo "EXTERNAL_PATH = $EXTERNAL_PATH"
    echo "KERBEROS_ENABLED = $EXTERNAL_ONLY_KERBERIZED"
  else
    echo "TABLE_STORAGE = $MANAGED_TABLE_STORAGE"
  fi
  echo "ITERATIONS = $ITERATIONS"
  echo "================================="
  echo
  exit 0
}

SHORT=p:,k:,d:,v:,b:,t:,s:,e:,E:,M:,i:,v:,u:,X:,K:,h
LONG=hp:,hk:,db:,vol:,bint:,tstmp:,struct:,ext:,esto:,msto:,iter:,hver:,huri:,xpath:,kerb:,help
OPTS=$(getopt --options $SHORT --longoptions $LONG -- "$@")

eval set -- "$OPTS"

while :; do
  case "$1" in
  -h | --help)
    help
    ;;
  -p | --hp)
    HIVE_PRINCIPAL="$2"
    shift 2
    ;;
  -k | --hk)
    HIVE_KEYTAB="$2"
    shift 2
    ;;
  -d | --db)
    DB_NAME="$2"
    shift 2
    ;;
  -v | --vol)
    VOLUME_1000="$2"
    shift 2
    ;;
  -b | --bint)
    BIGINT="$2"
    shift 2
    ;;
  -t | --tstmp)
    TIMESTAMP="$2"
    shift 2
    ;;
  -s | --struct)
    STRUCT="$2"
    shift 2
    ;;
  -e | --ext)
    EXTERNAL="$2"
    shift 2
    ;;
  -E | --esto)
    EXTERNAL_TABLE_STORAGE="$2"
    shift 2
    ;;
  -M | --msto)
    MANAGED_TABLE_STORAGE="$2"
    shift 2
    ;;
  -i | --iter)
    ITERATIONS="$2"
    shift 2
    ;;
  -v | --hver)
    HIVE_METASTORE_VERSION="$2"
    shift 2
    ;;
  -u | --huri)
    HIVE_METASTORE_URIS="$2"
    shift 2
    ;;
  -X | --xpath)
    EXTERNAL_PATH="$2"
    shift 2
    ;;
  -K | --kerb)
    EXTERNAL_ONLY_KERBERIZED="$2"
    shift 2
    ;;
  --)
    shift
    break
    ;;
  *)
    echo "Usage: $1 [--hp HIVE_PRINCIPAL] [--hk HIVE_KEYTAB] [--db DB_NAME] [--vol VOLUME_1000] [--bint BIGINT] [--tstmp TIMESTAMP] [--struct STRUCT] [--ext EXTERNAL] [--esto EXTERNAL_TABLE_STORAGE] [--msto MANAGED_TABLE_STORAGE] [--iter ITERATIONS]"
    echo
    echo "or run ./start.sh --help"
    echo
    exit 1
    ;;
  esac
done

echo
echo "================================="
echo "         CONFIG VALUES"
echo "================================="
echo
if [ "${EXTERNAL_ONLY_KERBERIZED,,}" = true ]; then
  echo "HIVE_PRINCIPAL = $HIVE_PRINCIPAL"
  echo "HIVE_KEYTAB = $HIVE_KEYTAB"
fi
echo "DB_NAME = $DB_NAME"
echo "VOLUME_1000 = $VOLUME_1000"
echo "BIGINT = $BIGINT"
echo "TIMESTAMP = $TIMESTAMP"
echo "STRUCT = $STRUCT"
echo "EXTERNAL = $EXTERNAL"
if [ "${EXTERNAL,,}" = true ]; then
  echo "TABLE_STORAGE = $EXTERNAL_TABLE_STORAGE"
  echo "HIVE_VERSION = $HIVE_METASTORE_VERSION"
  echo "HMS_URIs= $HIVE_METASTORE_URIS"
  echo "EXTERNAL_PATH = $EXTERNAL_PATH"
  echo "KERBEROS_ENABLED = $EXTERNAL_ONLY_KERBERIZED"
else
  echo "TABLE_STORAGE = $MANAGED_TABLE_STORAGE"
fi
echo "ITERATIONS = $ITERATIONS"
echo "================================="
echo

for ((i = 1; i <= "$ITERATIONS"; i++)); do
  if [ "$i" = 1 ]; then
    DB_NAME_ITER="$DB_NAME"
  else
    DB_NAME_ITER="$DB_NAME"_"$i"
  fi
  if [ "${EXTERNAL,,}" = true ]; then
    if [ ! -d ./spark-*-bin-hadoop* ]; then
      wget --no-check-certificate $SPARK_DL_LINK
      tar xzvf spark-*-bin*.tgz
      rm ./spark-*-bin*.tgz
      cp /etc/hadoop/conf/hdfs-site.xml ./spark-*-bin*/conf/
      cp /etc/hadoop/conf/core-site.xml ./spark-*-bin*/conf/
      cp /etc/hive/conf/hive-site.xml ./spark-*-bin*/conf/
      cp /etc/hadoop/conf/yarn-site.xml ./spark-*-bin*/conf/
      cp /etc/hive/conf/beeline-site.xml ./spark-*-bin*/conf/
    fi
    pip3 install virtualenv
    virtualenv env_spark -p /usr/bin/python3
    source env_spark/bin/activate
    pip3 install --upgrade pip
    pip3 install pyspark

    if [ "${EXTERNAL_ONLY_KERBERIZED,,}" != true ]; then
      sudo -u hive ./spark-*-bin*/bin/spark-submit \
        --packages org.apache.spark:spark-avro_2.13:3.5.2 \
        --conf spark.sql.catalogImplementation=hive \
        --conf spark.hadoop.hive.metastore.uris=$HIVE_METASTORE_URIS \
        --conf spark.sql.hive.metastore.version=$HIVE_METASTORE_VERSION \
        --conf spark.sql.hive.metastore.jars=maven \
        --conf spark.driver.memory=2G \
        --conf spark.log.level="ERROR" \
        external_main.py "$VOLUME_1000" "$BIGINT" "$TIMESTAMP" "$STRUCT" "$EXTERNAL_TABLE_STORAGE" "$DB_NAME_ITER" "$EXTERNAL_PATH"
    else
      ./spark-*-bin*/bin/spark-submit \
        --packages org.apache.spark:spark-avro_2.13:3.5.2 \
        --conf spark.sql.catalogImplementation=hive \
        --conf spark.kerberos.keytab=$HIVE_KEYTAB \
        --conf spark.kerberos.principal=$HIVE_PRINCIPAL \
        --conf spark.hadoop.hive.metastore.uris=$HIVE_METASTORE_URIS \
        --conf spark.sql.hive.metastore.version=$HIVE_METASTORE_VERSION \
        --conf spark.sql.hive.metastore.jars=maven \
        --conf spark.driver.memory=2G \
        --conf spark.log.level="ERROR" \
        external_main.py "$VOLUME_1000" "$BIGINT" "$TIMESTAMP" "$STRUCT" "$EXTERNAL_TABLE_STORAGE" "$DB_NAME_ITER" "$EXTERNAL_PATH"
    fi
  else
    python3 ./managed_main.py "$VOLUME_1000" "$BIGINT" "$TIMESTAMP" "$STRUCT" "$EXTERNAL" "$MANAGED_TABLE_STORAGE"

    kinit -kt "$HIVE_KEYTAB" "$HIVE_PRINCIPAL"
    KERBEROS=$?

    if [ "$KERBEROS" -eq 0 ]; then
      hive --hiveconf dbname="$DB_NAME_ITER" -f /tmp/CreateTables.sql
      kdestroy
    else
      sudo -u hive hive --hiveconf dbname="$DB_NAME_ITER" -f /tmp/CreateTables.sql
    fi

    rm /tmp/HiveDataIngest.sql
    rm /tmp/CreateTables.sql
  fi
done
