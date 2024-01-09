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

#include <memory>

#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {

StatusOr<std::unique_ptr<ColumnIterator>> create_json_flat_iterater(
        ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
        std::vector<std::unique_ptr<ColumnIterator>> field_iters, std::vector<std::string>& flat_paths,
        ColumnAccessPath* path);

StatusOr<std::unique_ptr<ColumnIterator>> create_json_dynamic_flat_iterater(
        std::unique_ptr<ScalarColumnIterator> json_iter, std::vector<std::string>& flat_paths, ColumnAccessPath* path);
} // namespace starrocks
