
# Spark JDBC Profiler

[![codecov](https://codecov.io/gh/hgbink/spark-jdbc-profiler/branch/main/graph/badge.svg?token=spark-jdbc-profiler_token_here)](https://codecov.io/gh/hgbink/spark-jdbc-profiler)
[![CI](https://github.com/hgbink/spark-jdbc-profiler/actions/workflows/main.yml/badge.svg)](https://github.com/hgbink/spark-jdbc-profiler/actions/workflows/main.yml)

Spark JDBC Profiler is a collection of utils functions for profiling source databases with spark jdbc connections. 


## Install it from PyPI

```bash
pip install spark_jdbc_profiler
```

## Usage

```py
from spark_jdbc_profiler.whole_db_profiler.mysql_db_profiler import *
from spark_jdbc_profiler.segmentation_profiler.segmentation_gen import *

jdbcUsername = "test_user"
jdbcPassword = "test_pass"
jdbcHostname = "mariadb"
jdbcPort = "3306"
jdbcDatabase = "test"

jdbcUrl = f"jdbc:mysql://{jdbcHostname}:{jdbcPort}/{jdbcDatabase}?zeroDateTimeBehavior=ROUND"
connectionProperties = {"user": jdbcUsername, "password": jdbcPassword}

df = profile_whole_db(spark, jdbcUrl, connectionProperties)
df.show(n=20)

```


## Development

Read the [CONTRIBUTING.md](CONTRIBUTING.md) file.
