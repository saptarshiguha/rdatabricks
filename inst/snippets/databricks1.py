import sys 
import datetime
import random
import subprocess
import logging
from s3logger import S3LogHandler
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
spark.conf.set('spark.databricks.queryWatchdog.maxQueryTasks', 140000)
spark.conf.set('spark.databricks.queryWatchdog.maxHivePartitions',32766)

## call log.info(text)
## and they will be displayed 
log = logging.getLogger("db")
hdlr = S3LogHandler(bucket='(__REPLACE__BUCKET)',
                    prefix='(__REPLACE__PREFIX)',
                    contextId = '(__REPLACE__CONTEXT)')
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
hdlr.setFormatter(formatter)
log.addHandler(hdlr)
log.setLevel(logging.INFO)

