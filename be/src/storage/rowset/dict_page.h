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

#include <functional>
#include <memory>
#include <string>

#include "gen_cpp/segment.pb.h"
#include "gutil/hash/string_hash.h"
#include "runtime/mem_pool.h"
#include "storage/olap_common.h"
#include "storage/range.h"
#include "storage/rowset/bitshuffle_page.h"
#include "storage/rowset/common.h"
#include "storage/rowset/options.h"
#include "storage/rowset/plain_page.h"
#include "storage/types.h"
#include "util/phmap/phmap.h"

namespace starrocks {

// This type of page use dictionary encoding for numbers.
// There is only one dictionary page for all the data pages within a column.
//
// Layout for dictionary encoded page:
// Either header + embedded codeword page, which can be encoded with any
//        int PageBuilder, when mode_ = DICT_ENCODING.
// Or     header + embedded BitshufflePageBuilder, when mode_ = PLAIN_ENCOING.
// Data pages start with mode_ = DICT_ENCODING, when the the size of dictionary
// page go beyond the option_->dict_page_size, the subsequent data pages will switch
// to string plain page automatically.

// DictPageBuilder has two encoders, data-page-builder and dict-builder
// dict-builder is used to encode the dictionary, data-page-builder is used to encode the
// dictionary's index. data-page-builder and dict-builder use BitshufflePageBuilder for encoding
// Because when the dictionary page is full, data-page-builder will no longer store the index of the
// dictionary page, but instead store the data itself, data-page-builder needs to reserve a segment
// of space in advance to store the encoding type, indicating whether this page stores the index of
// the dictionary page or the data
template <LogicalType Type>
class DictPageBuilder final : public PageBuilder {
public:
    explicit DictPageBuilder(const PageBuilderOptions& options);

    bool is_page_full() override;

    uint32_t add(const uint8_t* vals, uint32_t count) override;

    faststring* finish() override;

    void reset() override;

    uint32_t count() const override;

    uint64_t size() const override;

    faststring* get_dictionary_page() override;

    Status get_first_value(void* value) const override;

    Status get_last_value(void* value) const override;

    bool is_valid_global_dict(const GlobalDictMap* global_dict) const override;

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

    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };

    PageBuilderOptions _options;
    bool _finished;

    std::unique_ptr<PageBuilder> _data_page_builder;

    std::unique_ptr<BitshufflePageBuilder<Type>> _dict_builder;

    EncodingTypePB _encoding_type;
    // query for dict item -> dict id
    phmap::flat_hash_map<std::string, uint32_t, HashOfSlice, Eq> _dictionary;
    faststring _first_value;
};

// DictPageDecoder initially holds a segment of memory, and from the header of this memory segment,
// you can determine the encoding method used, whether it's DICT_ENCODING or BIT_SHUFFLE.
// When initializing DictPageDecoder, it does not load the dictionary page. The dictionary page provides
// an additional function set_dict_decoder, which sets the dict-decoder to BitshufflePageBuilder.
// When reading data, if the encoding method is BIT_SHUFFLE, you can directly load the data from the
// data-page-decoder. If it's not BIT_SHUFFLE, it means that the data-page does not store the actual data
// but rather the index of the data. In this case, you need to load the data from the dictionary.
template <LogicalType Type>
class DictPageDecoder final : public PageDecoder {
    using ValueType = typename CppTypeTraits<Type>::CppType;

public:
    DictPageDecoder(Slice data);

    [[nodiscard]] Status init() override;

    [[nodiscard]] Status seek_to_position_in_page(uint32_t pos) override;

    [[nodiscard]] Status next_batch(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_batch(const SparseRange<>& range, Column* dst) override;

    uint32_t count() const override { return _data_page_decoder->count(); }

    uint32_t current_index() const override { return _data_page_decoder->current_index(); }

    EncodingTypePB encoding_type() const override { return _encoding_type; }

    void set_dict_decoder(PageDecoder* dict_decoder);

    [[nodiscard]] Status next_dict_codes(size_t* n, Column* dst) override;

    [[nodiscard]] Status next_dict_codes(const SparseRange<>& range, Column* dst) override;

private:
    enum { SIZE_OF_TYPE = TypeTraits<Type>::size };
    Slice _data;
    std::unique_ptr<PageDecoder> _data_page_decoder;
    const BitShufflePageDecoder<Type>* _dict_decoder = nullptr;
    bool _parsed;
    EncodingTypePB _encoding_type;
    std::shared_ptr<Column> _vec_code_buf;
    uint32_t _max_value_legth = 0;
};

} // namespace starrocks
