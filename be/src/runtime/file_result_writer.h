// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/file_result_writer.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include "exec/parquet_builder.h"
#include "fs/fs.h"
#include "gen_cpp/DataSinks_types.h"
#include "runtime/result_writer.h"
#include "runtime/runtime_state.h"

namespace starrocks {

class FileSystem;
class ExprContext;
class FileBuilder;
class RuntimeProfile;
class WritableFile;

struct ResultFileOptions {
    std::string file_path;
    TFileFormatType::type file_format;
    std::string column_separator;
    std::string row_delimiter;
    size_t max_file_size_bytes = 1 * 1024 * 1024 * 1024; // 1GB
    std::vector<TNetworkAddress> broker_addresses;
    std::map<std::string, std::string> broker_properties;
    int write_buffer_size_kb;
    THdfsProperties hdfs_properties;
    bool use_broker;
    std::vector<std::string> file_column_names;
    ParquetBuilderOptions parquet_options;

    ResultFileOptions(const TResultFileSinkOptions& t_opt) {
        file_path = t_opt.file_path;
        file_format = t_opt.file_format;
        column_separator = t_opt.__isset.column_separator ? t_opt.column_separator : "\t";
        row_delimiter = t_opt.__isset.row_delimiter ? t_opt.row_delimiter : "\n";
        max_file_size_bytes = t_opt.__isset.max_file_size_bytes ? t_opt.max_file_size_bytes : max_file_size_bytes;

        if (t_opt.__isset.broker_addresses) {
            broker_addresses = t_opt.broker_addresses;
        }
        if (t_opt.__isset.hdfs_write_buffer_size_kb) {
            write_buffer_size_kb = t_opt.hdfs_write_buffer_size_kb;
        }
        if (t_opt.__isset.hdfs_properties) {
            hdfs_properties = t_opt.hdfs_properties;
        }
        if (t_opt.__isset.use_broker) {
            use_broker = t_opt.use_broker;
        }
        if (t_opt.__isset.broker_properties) {
            broker_properties = t_opt.broker_properties;
        }
        if (t_opt.__isset.file_column_names) {
            file_column_names = t_opt.file_column_names;
        }
        if (t_opt.__isset.parquet_options && t_opt.parquet_options.__isset.parquet_max_group_bytes) {
            parquet_options.row_group_max_size = t_opt.parquet_options.parquet_max_group_bytes;
        }
        if (t_opt.__isset.parquet_options && t_opt.parquet_options.__isset.use_dict) {
            parquet_options.use_dict = t_opt.parquet_options.use_dict;
        }
        if (t_opt.__isset.parquet_options && t_opt.parquet_options.__isset.compression_type) {
            parquet_options.compression_type = t_opt.parquet_options.compression_type;
        }
    }

    ~ResultFileOptions() = default;
};

// write result to file
class FileResultWriter final : public ResultWriter {
public:
    FileResultWriter(const ResultFileOptions* file_option, const std::vector<ExprContext*>& output_expr_ctxs,
                     RuntimeProfile* parent_profile);
    ~FileResultWriter() override;

    Status init(RuntimeState* state) override;
    Status append_chunk(Chunk* chunk) override;
    Status close() override;
    Status open(RuntimeState* state) override;

private:
    void _init_profile();

    Status _create_fs();

    Status _create_file_writer();
    // get next export file name
    std::string _get_next_file_name();
    std::string _file_format_to_name();
    // close file writer, and if !done, it will create new writer for next file
    Status _close_file_writer(bool done);
    // create a new file if current file size exceed limit
    Status _create_new_file_if_exceed_size();

    RuntimeState* _state = nullptr; // not owned, set when init
    const ResultFileOptions* _file_opts;
    const std::vector<ExprContext*>& _output_expr_ctxs;

    std::unique_ptr<FileSystem> _fs;
    std::unique_ptr<FileBuilder> _file_builder;

    // the suffix idx of export file name, start at 0
    int _file_idx = 0;

    RuntimeProfile* _parent_profile; // profile from result sink, not owned
    // total time cost on append chunk operation
    RuntimeProfile::Counter* _append_chunk_timer = nullptr;
    // tuple convert timer, child timer of _append_chunk_timer
    RuntimeProfile::Counter* _convert_tuple_timer = nullptr;
    // file write timer, child timer of _append_chunk_timer
    RuntimeProfile::Counter* _file_write_timer = nullptr;
    // time of closing the file writer
    RuntimeProfile::Counter* _writer_close_timer = nullptr;
    // number of written rows
    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    // bytes of written data
    RuntimeProfile::Counter* _written_data_bytes = nullptr;
};

} // namespace starrocks
