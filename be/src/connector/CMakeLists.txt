# This file is made available under Elastic License 2.0.

# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/connector")

add_library(Connector STATIC
    connector.cpp
    hive_connector.cpp
)