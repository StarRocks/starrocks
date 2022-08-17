# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/connector")

add_library(Connector STATIC
    connector.cpp
    hive_connector.cpp
    es_connector.cpp
    jdbc_connector.cpp
    lake_connector.cpp
    mysql_connector.cpp
    file_connector.cpp
)
