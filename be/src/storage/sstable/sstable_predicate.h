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

#include "storage/record_predicate/record_predicate.h"
#include "storage/sstable/sstable_predicate_fwd.h"
#include "storage/sstable/sstable_predicate_utils.h"

namespace starrocks {

namespace sstable {

class SstablePredicate {
public:
    SstablePredicate() : _record_predicate(nullptr), _converter(nullptr) {}
    ~SstablePredicate() = default;

    static StatusOr<SstablePredicateUPtr> create(const TabletSchemaPB& tablet_schema_pb,
                                                 const PersistentIndexSstablePredicatePB& predicate_pb);

    Status evaluate(const std::string& row, uint8_t* selection);

    bool equals(const SstablePredicate& other) const { return _record_predicate->equals(*other._record_predicate); }

private:
    Status _init(const PersistentIndexSstablePredicatePB& predicate_pb, KeyToChunkConverterUPtr& converter);

    RecordPredicateUPtr _record_predicate;
    KeyToChunkConverterUPtr _converter;
};

} // namespace sstable
} // namespace starrocks
