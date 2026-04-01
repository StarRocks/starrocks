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

if(NOT DEFINED STARROCKS_CODEGEN_MANIFEST)
    message(FATAL_ERROR "STARROCKS_CODEGEN_MANIFEST is required")
endif()

include("${STARROCKS_CODEGEN_MANIFEST}")

function(_starrocks_outputs_missing out_var)
    set(_missing FALSE)
    foreach(_output IN LISTS CODEGEN_OUTPUTS)
        if(NOT EXISTS "${_output}")
            set(_missing TRUE)
            break()
        endif()
    endforeach()
    set(${out_var} "${_missing}" PARENT_SCOPE)
endfunction()

set(_tool_version "")
execute_process(
    COMMAND "${CODEGEN_TOOL}" ${CODEGEN_VERSION_ARGS}
    RESULT_VARIABLE _version_result
    OUTPUT_VARIABLE _tool_version_stdout
    ERROR_VARIABLE _tool_version_stderr
    OUTPUT_STRIP_TRAILING_WHITESPACE
    ERROR_STRIP_TRAILING_WHITESPACE)
if(_version_result EQUAL 0)
    set(_tool_version "${_tool_version_stdout}${_tool_version_stderr}")
else()
    set(_tool_version "unknown")
endif()

set(_hash_input "tool=${CODEGEN_TOOL}\nversion=${_tool_version}\n")
foreach(_arg IN LISTS CODEGEN_GENERATOR_ARGS)
    string(APPEND _hash_input "arg=${_arg}\n")
endforeach()
foreach(_dependency IN LISTS CODEGEN_DEPENDENCIES)
    file(SHA256 "${_dependency}" _dependency_hash)
    string(APPEND _hash_input "dep=${_dependency}|${_dependency_hash}\n")
endforeach()
string(SHA256 _stamp_value "${_hash_input}")

file(MAKE_DIRECTORY "${CODEGEN_OUTPUT_DIR}")

_starrocks_outputs_missing(_missing_outputs)
if(EXISTS "${CODEGEN_STAMP}")
    file(READ "${CODEGEN_STAMP}" _existing_value)
    string(STRIP "${_existing_value}" _existing_value)
    if(_existing_value STREQUAL _stamp_value AND NOT _missing_outputs)
        execute_process(COMMAND "${CMAKE_COMMAND}" -E touch "${CODEGEN_STAMP}"
                        RESULT_VARIABLE _touch_result)
        if(NOT _touch_result EQUAL 0)
            message(FATAL_ERROR "Failed to touch stamp ${CODEGEN_STAMP}")
        endif()
        return()
    endif()
endif()

message(STATUS "Generating proto ${CODEGEN_SOURCE}")
execute_process(
    COMMAND "${CODEGEN_TOOL}" ${CODEGEN_GENERATOR_ARGS}
    RESULT_VARIABLE _generate_result
    OUTPUT_VARIABLE _generate_stdout
    ERROR_VARIABLE _generate_stderr)
if(NOT _generate_result EQUAL 0)
    message(FATAL_ERROR "Failed to generate proto ${CODEGEN_SOURCE}\n${_generate_stdout}\n${_generate_stderr}")
endif()

_starrocks_outputs_missing(_missing_outputs)
if(_missing_outputs)
    message(FATAL_ERROR "Proto generation completed without producing all outputs for ${CODEGEN_SOURCE}")
endif()

file(WRITE "${CODEGEN_STAMP}" "${_stamp_value}\n")
