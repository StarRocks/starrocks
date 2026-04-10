# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set(LIBRARY_OUTPUT_PATH "${BUILD_DIR}/src/geo")

# where to put generated binaries
set(EXECUTABLE_OUTPUT_PATH "${BUILD_DIR}/src/geo")

find_program(FLEX_EXECUTABLE NAMES flex REQUIRED)
if (APPLE)
    find_program(BISON_EXECUTABLE NAMES bison PATHS /opt/homebrew/opt/bison/bin /usr/local/opt/bison/bin NO_DEFAULT_PATH)
    if (NOT BISON_EXECUTABLE)
        find_program(BISON_EXECUTABLE NAMES bison)
    endif()
else()
    find_program(BISON_EXECUTABLE NAMES bison REQUIRED)
endif()

if (NOT BISON_EXECUTABLE)
    message(FATAL_ERROR "bison is required to generate BE geo parser sources")
endif()

execute_process(COMMAND ${BISON_EXECUTABLE} --version
                OUTPUT_VARIABLE BISON_VERSION_OUTPUT
                OUTPUT_STRIP_TRAILING_WHITESPACE)
string(REGEX MATCH "[0-9]+(\\.[0-9]+)+" BISON_VERSION "${BISON_VERSION_OUTPUT}")
if (APPLE AND BISON_VERSION VERSION_LESS "3.0.0")
    message(FATAL_ERROR "macOS BE build requires bison >= 3.0. Install Homebrew bison so ./build.sh --be can generate geo parser sources.")
endif()

ADD_BE_LIB(Geo
    geo_common.cpp
    geo_types.cpp
    wkt_parse.cpp
    ${GENSRC_DIR}/geo/wkt_lex.l.cpp
    ${GENSRC_DIR}/geo/wkt_yacc.y.cpp
)

if (CMAKE_CXX_COMPILER_ID STREQUAL "Clang")
    if (CMAKE_CXX_COMPILER_VERSION VERSION_GREATER_EQUAL "15.0.0")
        # ignore warning from geo
        target_compile_options(Geo PUBLIC "-Wno-unused-but-set-variable")
    endif ()
endif()

add_custom_command(
    OUTPUT ${GENSRC_DIR}/geo/wkt_lex.l.cpp ${GENSRC_DIR}/geo/wkt_lex.l.h
    COMMAND mkdir -p ${GENSRC_DIR}/geo
    COMMAND ${FLEX_EXECUTABLE} --header-file=${GENSRC_DIR}/geo/wkt_lex.l.h --outfile=${GENSRC_DIR}/geo/wkt_lex.l.cpp
    ${CMAKE_CURRENT_SOURCE_DIR}/wkt_lex.l
    MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/wkt_lex.l)
set_source_files_properties(${GENSRC_DIR}/geo/wkt_lex.l.cpp PROPERTIES GENERATED TRUE)

add_custom_command(
    OUTPUT ${GENSRC_DIR}/geo/wkt_yacc.y.cpp  ${GENSRC_DIR}/geo/wkt_yacc.y.hpp
    COMMAND mkdir -p ${GENSRC_DIR}/geo
    COMMAND ${BISON_EXECUTABLE} --output=${GENSRC_DIR}/geo/wkt_yacc.y.cpp ${CMAKE_CURRENT_SOURCE_DIR}/wkt_yacc.y
    MAIN_DEPENDENCY ${CMAKE_CURRENT_SOURCE_DIR}/wkt_yacc.y)
set_source_files_properties(${GENSRC_DIR}/geo/wkt_yacc.y.cpp PROPERTIES GENERATED TRUE)
