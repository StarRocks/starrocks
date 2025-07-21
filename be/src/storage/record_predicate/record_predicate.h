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

#include "gen_cpp/types.pb.h"

namespace starrocks {
class Chunk;
class Status;
/*
 * RecordPredicate is an abstract base class for predicates on chunk of data. All RecordPredicate
 * can be only evaluated in storage layer.
*/
class RecordPredicate;
using RecordPredicateSPtr = std::shared_ptr<RecordPredicate>;
using RecordPredicateUPtr = std::unique_ptr<RecordPredicate>;

class RecordPredicate {
public:
    RecordPredicate(const RecordPredicatePB& record_predicate_pb)
            : _pred_type(record_predicate_pb.type()), _children() {}
    virtual ~RecordPredicate() = default;

    // @Param chunk: the chunk to be evaluated
    // @Param selection: the selection vector to be filled, which indicates whether each row in the chunk
    // is selected (1) or not (0).
    // @Param from: the starting index of the chunk to evaluate
    // @Param to: the ending index of the chunk to evaluate
    // @Return: Status indicating success or failure of the evaluation.
    // The function without `from` and `to` parameters evaluates the entire chunk.
    virtual Status evaluate(Chunk* chunk, uint8_t* selection) const = 0;
    virtual Status evaluate(Chunk* chunk, uint8_t* selection, uint16_t from, uint16_t to) const = 0;

    virtual Status init(const RecordPredicatePB& record_predicate_pb) = 0;

    virtual const std::vector<std::string>* column_names() const = 0;

    virtual bool equals(const RecordPredicate& other) const = 0;

    RecordPredicatePB::RecordPredicateTypePB type() { return _pred_type; }

protected:
    RecordPredicatePB::RecordPredicateTypePB _pred_type;
    std::vector<RecordPredicateUPtr> _children;
};

} // namespace starrocks