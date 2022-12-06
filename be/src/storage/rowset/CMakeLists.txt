# where to put generated libraries
set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/olap/rowset")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/olap/rowset")

add_library(Rowset STATIC
    unique_rowset_id_generator.cpp
    rowset.cpp
    rowset_factory.cpp
    rowset_meta_manager.cpp
    rowset_writer.cpp
    )
