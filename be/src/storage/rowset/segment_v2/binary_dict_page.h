// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/segment_v2/binary_dict_page.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

#include <functional>
#include <memory>
#include <string>

#include "gen_cpp/segment_v2.pb.h"
#include "gutil/hash/string_hash.h"
#include "runtime/mem_pool.h"
#include "runtime/mem_tracker.h"
#include "storage/column_block.h"
#include "storage/column_vector.h"
#include "storage/olap_common.h"
#include "storage/rowset/segment_v2/binary_plain_page.h"
#include "storage/rowset/segment_v2/common.h"
#include "storage/rowset/segment_v2/options.h"
#include "storage/types.h"
#include "util/phmap/phmap.h"

namespace starrocks {
namespace segment_v2 {

enum { BINARY_DICT_PAGE_HEADER_SIZE = 4 };

// This type of page use dictionary encoding for strings.
// There is only one dictionary page for all the data pages within a column.
//
// Layout for dictionary encoded page:
// Either header + embedded codeword page, which can be encoded with any
//        int PageBuilder, when mode_ = DICT_ENCODING.
// Or     header + embedded BinaryPlainPage, when mode_ = PLAIN_ENCOING.
// Data pages start with mode_ = DICT_ENCODING, when the the size of dictionary
// page go beyond the option_->dict_page_size, the subsequent data pages will switch
// to string plain page automatically.
class BinaryDictPageBuilder final : public PageBuilder {
public:
    explicit BinaryDictPageBuilder(const PageBuilderOptions& options);

    bool is_page_full() override;

    size_t add(const uint8_t* vals, size_t count) override;

    faststring* finish() override;

    void reset() override;

    size_t count() const override;

    uint64_t size() const override;

    faststring* get_dictionary_page() override;

    Status get_first_value(void* value) const override;

    Status get_last_value(void* value) const override;

    bool is_valid_global_dict(const vectorized::GlobalDictMap* global_dict) const override;

    // Return true iff all pages so far are encoded by dictionary encoding.
    // this method normally should be called after all data pages finish
    // write, i.e, after `finish` has been called.
    bool all_dict_encoded() const override { return _encoding_type == DICT_ENCODING; }

private:
    struct HashOfSlice {
        // Enable heterogeneous lookup.
        typedef bool is_transparent;

        size_t operator()(const Slice& slice) const { return HashStringThoroughly(slice.data, slice.size); }

        size_t operator()(const std::string& s) const { return HashStringThoroughly(s.data(), s.size()); }
    };

    struct Eq {
        // Enable heterogeneous lookup.
        typedef bool is_transparent;

        bool operator()(const Slice& s1, const Slice& s2) const { return s1 == s2; }
    };

    PageBuilderOptions _options;
    bool _finished;

    std::unique_ptr<PageBuilder> _data_page_builder;

    std::unique_ptr<BinaryPlainPageBuilder> _dict_builder;

    EncodingTypePB _encoding_type;
    // query for dict item -> dict id
    phmap::flat_hash_map<std::string, uint32_t, HashOfSlice, Eq> _dictionary;
    // TODO(zc): rethink about this mem pool
    MemPool _pool;
    faststring _first_value;
};

template <FieldType Type>
class BinaryDictPageDecoder final : public PageDecoder {
public:
    BinaryDictPageDecoder(Slice data, const PageDecoderOptions& options);

    Status init() override;

    Status seek_to_position_in_page(size_t pos) override;

    Status next_batch(size_t* n, ColumnBlockView* dst) override;

    Status next_batch(size_t* n, vectorized::Column* dst) override;

    size_t count() const override { return _data_page_decoder->count(); }

    size_t current_index() const override { return _data_page_decoder->current_index(); }

    EncodingTypePB encoding_type() const override { return _encoding_type; }

    void set_dict_decoder(PageDecoder* dict_decoder);

    Status next_dict_codes(size_t* n, vectorized::Column* dst) override;

private:
    Slice _data;
    PageDecoderOptions _options;
    std::unique_ptr<PageDecoder> _data_page_decoder;
    const BinaryPlainPageDecoder<Type>* _dict_decoder = nullptr;
    bool _parsed;
    EncodingTypePB _encoding_type;
    std::unique_ptr<ColumnVectorBatch> _batch;
    std::shared_ptr<vectorized::Column> _vec_code_buf;

    uint32_t _max_value_legth = 0;
};

} // namespace segment_v2
} // namespace starrocks
