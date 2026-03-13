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

include_guard(GLOBAL)

function(starrocks_collect_proto_sources source_root out_var)
    file(GLOB _sources "${source_root}/*.proto")
    list(SORT _sources)
    set(${out_var} "${_sources}" PARENT_SCOPE)
endfunction()

function(starrocks_collect_thrift_sources source_root out_var)
    file(GLOB_RECURSE _sources LIST_DIRECTORIES FALSE "${source_root}/*.thrift")
    list(SORT _sources)
    set(${out_var} "${_sources}" PARENT_SCOPE)
endfunction()

function(_starrocks_get_codegen_source_id source_root source out_var)
    file(RELATIVE_PATH _relative_path "${source_root}" "${source}")
    string(REGEX REPLACE "\\.[^.]+$" "" _source_id "${_relative_path}")
    string(REPLACE "/" "_" _source_id "${_source_id}")
    string(REPLACE "\\" "_" _source_id "${_source_id}")
    string(REPLACE ":" "_" _source_id "${_source_id}")
    set(${out_var} "${_source_id}" PARENT_SCOPE)
endfunction()

function(_starrocks_validate_unique_thrift_outputs output_dir)
    set(_entries)
    foreach(_source IN LISTS ARGN)
        starrocks_get_thrift_outputs("${output_dir}" "${_source}" _outputs)
        foreach(_output IN LISTS _outputs)
            list(APPEND _entries "${_output}|${_source}")
        endforeach()
    endforeach()

    list(SORT _entries)
    set(_previous_output "")
    set(_previous_source "")
    foreach(_entry IN LISTS _entries)
        string(FIND "${_entry}" "|" _separator_index)
        math(EXPR _source_index "${_separator_index} + 1")
        string(SUBSTRING "${_entry}" 0 ${_separator_index} _output)
        string(SUBSTRING "${_entry}" ${_source_index} -1 _source)

        if(_output STREQUAL _previous_output)
            message(FATAL_ERROR
                    "Conflicting thrift output ${_output} from ${_previous_source} and ${_source}. "
                    "Generated thrift outputs are flat, so thrift basenames and service names must "
                    "be unique within a codegen target.")
        endif()

        set(_previous_output "${_output}")
        set(_previous_source "${_source}")
    endforeach()
endfunction()

function(_starrocks_collect_proto_recursive source source_root visited_in result_var visited_out_var)
    set(_visited "${visited_in}")
    list(FIND _visited "${source}" _visited_index)
    if(NOT _visited_index EQUAL -1)
        set(${result_var} "" PARENT_SCOPE)
        set(${visited_out_var} "${_visited}" PARENT_SCOPE)
    else()
        list(APPEND _visited "${source}")
        set(_deps "${source}")

        file(STRINGS "${source}" _import_lines REGEX "^[ \t]*import[ \t]+\"[^\"]+\";")
        foreach(_line IN LISTS _import_lines)
            string(REGEX REPLACE "^[ \t]*import[ \t]+\"([^\"]+)\";.*" "\\1" _relative_path "${_line}")
            set(_dependency "${source_root}/${_relative_path}")
            if(NOT EXISTS "${_dependency}")
                message(FATAL_ERROR "Missing proto dependency ${_relative_path} referenced by ${source}")
            endif()

            _starrocks_collect_proto_recursive("${_dependency}" "${source_root}" "${_visited}"
                                              _nested_dependencies _visited)
            list(APPEND _deps ${_nested_dependencies})
        endforeach()

        list(REMOVE_DUPLICATES _deps)
        set(${result_var} "${_deps}" PARENT_SCOPE)
        set(${visited_out_var} "${_visited}" PARENT_SCOPE)
    endif()
endfunction()

function(starrocks_collect_proto_dependency_closure source_root source out_var)
    _starrocks_collect_proto_recursive("${source}" "${source_root}" "" _dependencies _visited)
    set(${out_var} "${_dependencies}" PARENT_SCOPE)
endfunction()

function(_starrocks_collect_thrift_recursive source source_root visited_in result_var visited_out_var)
    set(_visited "${visited_in}")
    list(FIND _visited "${source}" _visited_index)
    if(NOT _visited_index EQUAL -1)
        set(${result_var} "" PARENT_SCOPE)
        set(${visited_out_var} "${_visited}" PARENT_SCOPE)
    else()
        list(APPEND _visited "${source}")
        set(_deps "${source}")
        get_filename_component(_source_dir "${source}" DIRECTORY)

        file(STRINGS "${source}" _include_lines REGEX "^[ \t]*include[ \t]+\"[^\"]+\"")
        foreach(_line IN LISTS _include_lines)
            string(REGEX REPLACE "^[ \t]*include[ \t]+\"([^\"]+)\".*" "\\1" _relative_path "${_line}")
            get_filename_component(_dependency "${_source_dir}/${_relative_path}" ABSOLUTE)
            if(NOT EXISTS "${_dependency}")
                get_filename_component(_dependency "${source_root}/${_relative_path}" ABSOLUTE)
            endif()
            if(NOT EXISTS "${_dependency}")
                message(FATAL_ERROR "Missing thrift dependency ${_relative_path} referenced by ${source}")
            endif()

            _starrocks_collect_thrift_recursive("${_dependency}" "${source_root}" "${_visited}"
                                                _nested_dependencies _visited)
            list(APPEND _deps ${_nested_dependencies})
        endforeach()

        list(REMOVE_DUPLICATES _deps)
        set(${result_var} "${_deps}" PARENT_SCOPE)
        set(${visited_out_var} "${_visited}" PARENT_SCOPE)
    endif()
endfunction()

function(starrocks_collect_thrift_dependency_closure source_root source out_var)
    _starrocks_collect_thrift_recursive("${source}" "${source_root}" "" _dependencies _visited)
    set(${out_var} "${_dependencies}" PARENT_SCOPE)
endfunction()

function(starrocks_get_proto_outputs output_dir source out_var)
    get_filename_component(_base_name "${source}" NAME_WE)
    set(${out_var}
        "${output_dir}/${_base_name}.pb.h"
        "${output_dir}/${_base_name}.pb.cc"
        PARENT_SCOPE)
endfunction()

function(starrocks_get_thrift_outputs output_dir source out_var)
    get_filename_component(_base_name "${source}" NAME_WE)

    set(_outputs
        "${output_dir}/${_base_name}_types.h"
        "${output_dir}/${_base_name}_types.cpp")

    file(STRINGS "${source}" _const_lines REGEX "^[ \t]*const[ \t]")
    if(_const_lines)
        list(APPEND _outputs
             "${output_dir}/${_base_name}_constants.h"
             "${output_dir}/${_base_name}_constants.cpp")
    endif()

    file(STRINGS "${source}" _service_lines REGEX "^[ \t]*service[ \t]+[A-Za-z_][A-Za-z0-9_]*")
    foreach(_line IN LISTS _service_lines)
        string(REGEX REPLACE "^[ \t]*service[ \t]+([A-Za-z_][A-Za-z0-9_]*).*" "\\1" _service_name "${_line}")
        list(APPEND _outputs
             "${output_dir}/${_service_name}.h"
             "${output_dir}/${_service_name}.cpp"
             "${output_dir}/${_service_name}_server.skeleton.cpp")
    endforeach()

    list(REMOVE_DUPLICATES _outputs)
    set(${out_var} "${_outputs}" PARENT_SCOPE)
endfunction()

function(_starrocks_write_manifest_list manifest_path variable_name)
    file(APPEND "${manifest_path}" "set(${variable_name}\n")
    foreach(_entry IN LISTS ARGN)
        file(APPEND "${manifest_path}" "    [=[${_entry}]=]\n")
    endforeach()
    file(APPEND "${manifest_path}" ")\n")
endfunction()

function(_starrocks_write_codegen_manifest manifest_path)
    cmake_parse_arguments(ARG
        ""
        "SOURCE;SOURCE_ROOT;OUTPUT_DIR;STAMP;TOOL;PATCH_FILE;PATCH_HEADER"
        "DEPENDENCIES;OUTPUTS;GENERATOR_ARGS;VERSION_ARGS"
        ${ARGN})

    get_filename_component(_manifest_dir "${manifest_path}" DIRECTORY)
    file(MAKE_DIRECTORY "${_manifest_dir}")

    file(WRITE "${manifest_path}" "")
    file(APPEND "${manifest_path}" "set(CODEGEN_SOURCE [=[${ARG_SOURCE}]=])\n")
    file(APPEND "${manifest_path}" "set(CODEGEN_SOURCE_ROOT [=[${ARG_SOURCE_ROOT}]=])\n")
    file(APPEND "${manifest_path}" "set(CODEGEN_OUTPUT_DIR [=[${ARG_OUTPUT_DIR}]=])\n")
    file(APPEND "${manifest_path}" "set(CODEGEN_STAMP [=[${ARG_STAMP}]=])\n")
    file(APPEND "${manifest_path}" "set(CODEGEN_TOOL [=[${ARG_TOOL}]=])\n")
    file(APPEND "${manifest_path}" "set(CODEGEN_PATCH_FILE [=[${ARG_PATCH_FILE}]=])\n")
    file(APPEND "${manifest_path}" "set(CODEGEN_PATCH_HEADER [=[${ARG_PATCH_HEADER}]=])\n")
    _starrocks_write_manifest_list("${manifest_path}" "CODEGEN_DEPENDENCIES" ${ARG_DEPENDENCIES})
    _starrocks_write_manifest_list("${manifest_path}" "CODEGEN_OUTPUTS" ${ARG_OUTPUTS})
    _starrocks_write_manifest_list("${manifest_path}" "CODEGEN_GENERATOR_ARGS" ${ARG_GENERATOR_ARGS})
    _starrocks_write_manifest_list("${manifest_path}" "CODEGEN_VERSION_ARGS" ${ARG_VERSION_ARGS})
endfunction()

function(_starrocks_run_codegen_script script_path manifest_path)
    execute_process(
        COMMAND "${CMAKE_COMMAND}" "-DSTARROCKS_CODEGEN_MANIFEST=${manifest_path}" -P "${script_path}"
        RESULT_VARIABLE _result
        OUTPUT_VARIABLE _stdout
        ERROR_VARIABLE _stderr)
    if(NOT _result EQUAL 0)
        message(FATAL_ERROR
                "Failed to generate code using ${script_path}\nManifest: ${manifest_path}\n${_stdout}\n${_stderr}")
    endif()
endfunction()

function(starrocks_define_proto_codegen_target target_name)
    cmake_parse_arguments(ARG
        "CONFIGURE_GENERATE"
        "SOURCE_ROOT;OUTPUT_DIR;MANIFEST_DIR;SCRIPT;TOOL"
        ""
        ${ARGN})

    if(NOT EXISTS "${ARG_TOOL}")
        message(FATAL_ERROR "protoc compiler not found: ${ARG_TOOL}")
    endif()

    file(MAKE_DIRECTORY "${ARG_OUTPUT_DIR}")
    file(MAKE_DIRECTORY "${ARG_MANIFEST_DIR}")

    starrocks_collect_proto_sources("${ARG_SOURCE_ROOT}" _sources)
    set(_stamps)
    foreach(_source IN LISTS _sources)
        get_filename_component(_base_name "${_source}" NAME_WE)
        starrocks_collect_proto_dependency_closure("${ARG_SOURCE_ROOT}" "${_source}" _dependencies)
        starrocks_get_proto_outputs("${ARG_OUTPUT_DIR}" "${_source}" _outputs)

        set(_stamp "${ARG_OUTPUT_DIR}/.${_base_name}_proto.stamp")
        set(_manifest "${ARG_MANIFEST_DIR}/${_base_name}_proto_manifest.cmake")
        _starrocks_write_codegen_manifest(
            "${_manifest}"
            SOURCE "${_source}"
            SOURCE_ROOT "${ARG_SOURCE_ROOT}"
            OUTPUT_DIR "${ARG_OUTPUT_DIR}"
            STAMP "${_stamp}"
            TOOL "${ARG_TOOL}"
            DEPENDENCIES ${_dependencies}
            OUTPUTS ${_outputs}
            VERSION_ARGS "--version"
            GENERATOR_ARGS "--proto_path=${ARG_SOURCE_ROOT}" "--cpp_out=${ARG_OUTPUT_DIR}" "${_source}")

        add_custom_command(
            OUTPUT "${_stamp}"
            BYPRODUCTS ${_outputs}
            COMMAND "${CMAKE_COMMAND}" "-DSTARROCKS_CODEGEN_MANIFEST=${_manifest}" -P "${ARG_SCRIPT}"
            DEPENDS ${_dependencies} "${_manifest}" "${ARG_SCRIPT}" "${ARG_TOOL}"
            COMMENT "Checking proto generation for ${_base_name}.proto"
            VERBATIM)

        if(ARG_CONFIGURE_GENERATE)
            _starrocks_run_codegen_script("${ARG_SCRIPT}" "${_manifest}")
        endif()

        list(APPEND _stamps "${_stamp}")
    endforeach()

    add_custom_target(${target_name} DEPENDS ${_stamps})
endfunction()

function(starrocks_define_thrift_codegen_target target_name)
    cmake_parse_arguments(ARG
        "CONFIGURE_GENERATE"
        "SOURCE_ROOT;OUTPUT_DIR;MANIFEST_DIR;SCRIPT;TOOL"
        ""
        ${ARGN})

    if(NOT EXISTS "${ARG_TOOL}")
        message(FATAL_ERROR "thrift compiler not found: ${ARG_TOOL}")
    endif()

    file(MAKE_DIRECTORY "${ARG_OUTPUT_DIR}")
    file(MAKE_DIRECTORY "${ARG_MANIFEST_DIR}")

    starrocks_collect_thrift_sources("${ARG_SOURCE_ROOT}" _sources)
    _starrocks_validate_unique_thrift_outputs("${ARG_OUTPUT_DIR}" ${_sources})
    set(_stamps)
    foreach(_source IN LISTS _sources)
        get_filename_component(_base_name "${_source}" NAME_WE)
        _starrocks_get_codegen_source_id("${ARG_SOURCE_ROOT}" "${_source}" _source_id)
        starrocks_collect_thrift_dependency_closure("${ARG_SOURCE_ROOT}" "${_source}" _dependencies)
        starrocks_get_thrift_outputs("${ARG_OUTPUT_DIR}" "${_source}" _outputs)

        set(_patch_file "")
        set(_patch_header "")
        if(_base_name STREQUAL "StatusCode")
            set(_patch_file "${ARG_SOURCE_ROOT}/StatusCode_types.diff")
            set(_patch_header "${ARG_OUTPUT_DIR}/StatusCode_types.h")
            list(APPEND _dependencies "${_patch_file}")
        endif()

        set(_stamp "${ARG_OUTPUT_DIR}/.${_source_id}_thrift.stamp")
        set(_manifest "${ARG_MANIFEST_DIR}/${_source_id}_thrift_manifest.cmake")
        _starrocks_write_codegen_manifest(
            "${_manifest}"
            SOURCE "${_source}"
            SOURCE_ROOT "${ARG_SOURCE_ROOT}"
            OUTPUT_DIR "${ARG_OUTPUT_DIR}"
            STAMP "${_stamp}"
            TOOL "${ARG_TOOL}"
            PATCH_FILE "${_patch_file}"
            PATCH_HEADER "${_patch_header}"
            DEPENDENCIES ${_dependencies}
            OUTPUTS ${_outputs}
            VERSION_ARGS "-version"
            GENERATOR_ARGS "-I" "${ARG_SOURCE_ROOT}" "--allow-64bit-consts"
                           "--gen" "cpp" "-out" "${ARG_OUTPUT_DIR}" "${_source}")

        add_custom_command(
            OUTPUT "${_stamp}"
            BYPRODUCTS ${_outputs}
            COMMAND "${CMAKE_COMMAND}" "-DSTARROCKS_CODEGEN_MANIFEST=${_manifest}" -P "${ARG_SCRIPT}"
            DEPENDS ${_dependencies} "${_manifest}" "${ARG_SCRIPT}" "${ARG_TOOL}"
            COMMENT "Checking thrift generation for ${_base_name}.thrift"
            VERBATIM)

        if(ARG_CONFIGURE_GENERATE)
            _starrocks_run_codegen_script("${ARG_SCRIPT}" "${_manifest}")
        endif()

        list(APPEND _stamps "${_stamp}")
    endforeach()

    add_custom_target(${target_name} DEPENDS ${_stamps})
endfunction()
