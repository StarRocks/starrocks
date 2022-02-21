# This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.


# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/object_store")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/object_store")

set(EXEC_FILES placeholder.cpp)

if (WITH_AWS)
set(EXEC_FILES ${EXEC_FILES}
        s3_object_store.cpp)
endif()

add_library(ObjectStore STATIC
    ${EXEC_FILES}
)
