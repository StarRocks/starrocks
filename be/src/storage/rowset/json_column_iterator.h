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
#include "storage/rowset/page_decoder.h"
#include "storage/rowset/scalar_column_iterator.h"

namespace starrocks {

/**
 * @brief Creates a ColumnIterator that flattens a JSON column into multiple subfields according to the specified target and source paths/types.
 *
 * This iterator is used for reading flat JSON columns, mapping source JSON subfields to target columns, and optionally including remaining fields.
 *
 * @param reader Pointer to the ColumnReader for the JSON column.
 * @param null_iter Iterator for the null column (if nullable), or nullptr.
 * @param iters Iterators for each source subfield column.
 * @param target_paths List of target JSON paths to extract.
 * @param target_types List of logical types for each target path.
 * @param source_paths List of source JSON paths present in the storage.
 * @param source_types List of logical types for each source path.
 * @param need_remain Whether to include remaining fields not mapped to target paths.
 * @return StatusOr containing a unique pointer to the created ColumnIterator on success, or an error status.
 */
StatusOr<std::unique_ptr<ColumnIterator>> create_json_flat_iterator(
        ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
        std::vector<std::unique_ptr<ColumnIterator>> iters, const std::vector<std::string>& target_paths,
        const std::vector<LogicalType>& target_types, const std::vector<std::string>& source_paths,
        const std::vector<LogicalType>& source_types, bool need_remain = false);

/**
 * @brief Creates a ColumnIterator that dynamically flattens a JSON column at read time based on the specified target paths/types.
 *
 * This iterator is used when the structure of the JSON is not known at write time, and flattening is performed on-the-fly during reads.
 *
 * @param json_iter Iterator for the raw JSON column.
 * @param target_paths List of target JSON paths to extract.
 * @param target_types List of logical types for each target path.
 * @param need_remain Whether to include remaining fields not mapped to target paths.
 * @return StatusOr containing a unique pointer to the created ColumnIterator on success, or an error status.
 */
StatusOr<std::unique_ptr<ColumnIterator>> create_json_dynamic_flat_iterator(
        std::unique_ptr<ScalarColumnIterator> json_iter, const std::vector<std::string>& target_paths,
        const std::vector<LogicalType>& target_types, bool need_remain = false);

/**
 * @brief Creates a ColumnIterator that merges multiple JSON subfield columns into a single JSON column.
 *
 * This iterator is used for reconstructing a full JSON object from its flattened subfields during reads.
 *
 * @param reader Pointer to the ColumnReader for the JSON column.
 * @param null_iter Iterator for the null column (if nullable), or nullptr.
 * @param all_iters Iterators for all subfield columns to be merged.
 * @param merge_paths List of JSON paths to merge.
 * @param merge_types List of logical types for each merge path.
 * @return StatusOr containing a unique pointer to the created ColumnIterator on success, or an error status.
 */
StatusOr<std::unique_ptr<ColumnIterator>> create_json_merge_iterator(
        ColumnReader* reader, std::unique_ptr<ColumnIterator> null_iter,
        std::vector<std::unique_ptr<ColumnIterator>> all_iters, const std::vector<std::string>& merge_paths,
        const std::vector<LogicalType>& merge_types);

/**
 * @brief create a ColumnIterator that extract a field from the underlying json column
 */
StatusOr<ColumnIteratorUPtr> create_json_extract_iterator(ColumnIteratorUPtr source_iter, bool source_nullable,
                                                          const std::string& path, LogicalType type);

} // namespace starrocks
