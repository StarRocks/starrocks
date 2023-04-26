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

#pragma once

#include <cstdint>
#include <map>
#include <string>
#include <utility>

#include "exec/file_builder.h"

namespace starrocks {

namespace csv {
class Converter;
class OutputStream;
} // namespace csv

class ExprContext;
class FileWriter;

struct PlainTextBuilderOptions {
    std::string column_terminated_by;
    std::string line_terminated_by;
};

class PlainTextBuilder final : public FileBuilder {
public:
    PlainTextBuilder(PlainTextBuilderOptions options, std::unique_ptr<WritableFile> writable_file,
                     const std::vector<ExprContext*>& output_expr_ctxs);
    ~PlainTextBuilder() override = default;

    Status add_chunk(Chunk* chunk) override;

    std::size_t file_size() override;

    Status finish() override;

private:
    const static size_t OUTSTREAM_BUFFER_SIZE_BYTES;
    const PlainTextBuilderOptions _options;
    const std::vector<ExprContext*>& _output_expr_ctxs;
    std::unique_ptr<csv::OutputStream> _output_stream;
    std::vector<std::unique_ptr<csv::Converter>> _converters;
    bool _init;

    Status init();
};

} // namespace starrocks
