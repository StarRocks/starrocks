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

#include "storage/table_reader.h"

namespace starrocks {

TableReader::TableReader(const TableReaderParams& params) : _params(params) {}

TableReader::~TableReader() {}

Status TableReader::multi_get(const Chunk& keys, const std::vector<std::string>& value_columns,
                              std::vector<bool>& found, Chunk& values) {
    // TODO implement multi_get
    return Status::OK();
}

StatusOr<ChunkIteratorPtr> TableReader::scan(const std::vector<std::string>& value_columns,
                                             const std::vector<ColumnPredicate*>& predicates) {
    // TODO implement scan
    return Status::OK();
}

} // namespace starrocks