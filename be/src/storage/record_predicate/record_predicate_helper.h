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

#include "storage/olap_common.h"
#include "storage/record_predicate/record_predicate.h"

namespace starrocks {
class TabletSchema;
class Schema;

template <typename T>
class StatusOr;

class RecordPredicateHelper {
public:
    static StatusOr<RecordPredicateUPtr> create(const RecordPredicatePB& record_predicate_pb);

    static Status check_valid_schema(const RecordPredicate& predicate, const Schema& chunk_schema);
    static Status get_column_ids(const RecordPredicate& predicate,
                                 const std::shared_ptr<const TabletSchema>& tablet_schema,
                                 std::set<ColumnId>* column_ids);
    static Status get_column_ids(const RecordPredicate& predicate, const Schema& schema,
                                 std::set<ColumnId>* column_ids);

    static std::string from_type_to_string(RecordPredicatePB::RecordPredicateTypePB pred_type);

private:
    static Status _check_valid_pb(const RecordPredicatePB& record_predicate_pb);
};

} // namespace starrocks