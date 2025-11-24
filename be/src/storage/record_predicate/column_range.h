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

#include "google/protobuf/util/message_differencer.h"
#include "storage/record_predicate/record_predicate.h"
#include "storage/column_predicate.h"

namespace starrocks {

class Schema;

/*
 * ColumnRange is used to evaluate a chunk of data as following:
 * 1. Given the raw value of columns with the specified columns.
 * 2. Check if the value is in the range or not.
 * 3. The range is a lexicographic tuple range, which is a range of values that are ordered by the values of the columns.
 */
class ColumnRange final : public RecordPredicate {
public:
    using ColumnRangeMeta = RecordPredicatePB::ColumnRangePB;

    ColumnRange(const RecordPredicatePB& record_predicate_pb);
    ~ColumnRange() override = default;

    Status evaluate(Chunk* chunk, uint8_t* selection) const override;

    Status evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const override;

    Status init(const RecordPredicatePB& record_predicate_pb) override;

    const std::vector<std::string>* column_names() const override { return &_column_names; }

    bool equals(const RecordPredicate& other) const override {
        if (typeid(*this) != typeid(other)) {
            return false;
        }
        const auto& other_predicate = static_cast<const ColumnRange&>(other);
        return _column_names == other_predicate._column_names && 
               google::protobuf::util::MessageDifferencer::Equals(_range, other_predicate._range);
    }

private:
    Status _check_valid_pb(const ColumnRangeMeta& column_range_meta) const;

    Status _init_predicates(const Schema& chunk_schema) const;

    TabletRangePB _range;
    mutable PredicateType _predicate_type;
    mutable std::vector<std::unique_ptr<ColumnPredicate>> _predicates;
    mutable std::vector<std::unique_ptr<ColumnPredicate>> _eq_predicates;
    std::vector<std::string> _column_names;
    mutable std::vector<std::vector<uint8_t>> _selection_vectors;
    mutable std::vector<std::vector<uint8_t>> _eq_selection_vectors;
    static constexpr size_t _APPROXIMATE_EVAL_ROWS = 4096;
};

} // namespace starrocks