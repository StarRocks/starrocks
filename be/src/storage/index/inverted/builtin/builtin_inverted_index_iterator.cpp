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

#include "storage/index/inverted/builtin/builtin_inverted_index_iterator.h"

#include <CLucene.h>

#include <memory>

#include "exprs/function_context.h"
#include "exprs/like_predicate.h"

namespace starrocks {

BuiltinInvertedIndexIterator::~BuiltinInvertedIndexIterator() {
    if (_like_context) {
        (void) LikePredicate::like_close(_like_context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL);
    }
}

Status BuiltinInvertedIndexIterator::_init_like_context(const Slice& s) {
    _like_context = std::make_unique<FunctionContext>();
    auto ptr = BinaryColumn::create();
    ptr->append_datum(Datum(s));
    ptr->get_data();
    Columns cols;
    cols.push_back(nullptr);
    cols.push_back(std::move(ConstColumn::create(std::move(ptr), 1)));
    _like_context->set_constant_columns(cols);
    return LikePredicate::like_prepare(_like_context.get(), FunctionContext::FunctionStateScope::THREAD_LOCAL);
}

Status BuiltinInvertedIndexIterator::read_from_inverted_index(const std::string& column_name, const void* query_value,
                                                       InvertedIndexQueryType query_type, roaring::Roaring* bit_map) {
    const auto* search_query = reinterpret_cast<const Slice*>(query_value);
    switch (query_type) {
    case InvertedIndexQueryType::EQUAL_QUERY: {
        bool exact_match = false;
        Status st = _bitmap_itr->seek_dictionary(search_query, &exact_match);
        if (st.ok() && exact_match) {
            rowid_t ordinal = _bitmap_itr->current_ordinal();
            RETURN_IF_ERROR(_bitmap_itr->read_bitmap(ordinal, bit_map));
        } else if (!st.ok() && !st.is_not_found()) {
            return st;
        }
        break;
    }
    case InvertedIndexQueryType::MATCH_WILDCARD_QUERY: {
        RETURN_IF_ERROR(_init_like_context(*search_query));
        auto predicate = [this](const Column& value_column) -> StatusOr<ColumnPtr> {
            Columns cols;
            cols.push_back(&value_column);
            cols.push_back(this->_like_context->get_constant_column(1));
            return LikePredicate::like(this->_like_context.get(), cols);
        };
        auto res = _bitmap_itr->seek_all_dictionary(predicate);
        Status st = res.status();
        if (st.ok()) {
            auto hit_rowids = std::move(res.value());
            RETURN_IF_ERROR(_bitmap_itr->read_union_bitmap(hit_rowids, bit_map));
        } else if (!st.ok() && !st.is_not_found()) {
            return st;
        }
        break;
    }
    default:
        return Status::InvalidArgument("do not support query type");
    }
    return Status::OK();
}

Status BuiltinInvertedIndexIterator::read_null(const std::string& column_name, roaring::Roaring* bit_map) {
    return Status::InternalError("Unsupported");
}

} // namespace starrocks
