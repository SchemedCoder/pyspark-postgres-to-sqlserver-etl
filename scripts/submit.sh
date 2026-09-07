#!/bin/bash
echo "Clear the runway! Spark job incoming..."

# Submit the job with the required JDBC driver jars attached
spark-submit \
    --master local[*] \
    --jars jars/postgresql-42.6.0.jar,jars/mssql-jdbc-12.4.2.jre11.jar \
    --driver-memory 4g \
    main.py
