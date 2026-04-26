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

#include "formats/file_writer.h"

#include <fmt/format.h>

#include <utility>

#include "io/async_flush_output_stream.h"

namespace starrocks::formats {

FileWriter::CommitResult& FileWriter::CommitResult::set_extra_data(std::string extra_data) {
    this->extra_data = std::move(extra_data);
    return *this;
}

FileWriter::CommitResult& FileWriter::CommitResult::set_referenced_data_file(std::string referenced_data_file) {
    this->referenced_data_file = std::move(referenced_data_file);
    return *this;
}

UnknownFileWriterFactory::UnknownFileWriterFactory(std::string format) : _format(std::move(format)) {}

Status UnknownFileWriterFactory::init() {
    return Status::NotSupported(fmt::format("got unsupported file format: {}", _format));
}

StatusOr<WriterAndStream> UnknownFileWriterFactory::create(const std::string& path) const {
    return Status::NotSupported(fmt::format("got unsupported file format: {}", _format));
}

} // namespace starrocks::formats
