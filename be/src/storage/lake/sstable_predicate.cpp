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

#include "storage/lake/sstable_predicate.h"

#include "storage/record_predicate/record_predicate_helper.h"

namespace starrocks::lake {

StatusOr<SstablePredicateUPtr> SstablePredicate::create(const TabletSchemaPB& tablet_schema_pb,
                                                        const PersistentIndexSstablePredicatePB& sstable_predicate_pb) {
    ASSIGN_OR_RETURN(auto converter, KeyToChunkConverter::create(tablet_schema_pb));
    auto predicate = std::make_unique<SstablePredicate>();
    RETURN_IF_ERROR(predicate->_init(sstable_predicate_pb, converter));
    return predicate;
}

Status SstablePredicate::evaluate(const std::string& row, uint8_t* selection) {
    ASSIGN_OR_RETURN(auto chunk, _converter->convert_to_chunk(row));
    DCHECK(chunk->num_rows() == 1);
    RETURN_IF_ERROR(_record_predicate->evaluate(chunk.get(), selection));
    return Status::OK();
}

Status SstablePredicate::_init(const PersistentIndexSstablePredicatePB& predicate_pb,
                               KeyToChunkConverterUPtr& converter) {
    ASSIGN_OR_RETURN(_record_predicate, RecordPredicateHelper::create(predicate_pb.record_predicate()));
    _converter = std::move(converter);
    return Status::OK();
}

} // namespace starrocks::lake
