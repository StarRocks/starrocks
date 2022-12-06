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

#include "storage/rowset/segment.h"

namespace starrocks {

class ShortKeyIndexGroupIterator;
class ShortKeyIndexDecoderGroup;
class SegmentGroup;
using SegmentGroupPtr = std::unique_ptr<SegmentGroup>;

class ShortKeyIndexGroupIterator {
public:
    using iterator_category = std::random_access_iterator_tag;
    using value_type = Slice;
    using pointer = Slice*;
    using reference = Slice&;
    using difference_type = ssize_t;

    ShortKeyIndexGroupIterator() : _decoder_group(nullptr), _ordinal(0) {}

    ShortKeyIndexGroupIterator(const ShortKeyIndexDecoderGroup* decoder_group, ssize_t ordinal);

    ShortKeyIndexGroupIterator& operator-=(ssize_t step) {
        _ordinal -= step;
        return *this;
    }

    ShortKeyIndexGroupIterator& operator+=(ssize_t step) {
        _ordinal += step;
        return *this;
    }

    ShortKeyIndexGroupIterator& operator++() {
        ++_ordinal;
        return *this;
    }

    ShortKeyIndexGroupIterator& operator--() {
        --_ordinal;
        return *this;
    }

    bool operator!=(const ShortKeyIndexGroupIterator& rhs) const {
        return _ordinal != rhs._ordinal || _decoder_group != rhs._decoder_group;
    }

    bool operator==(const ShortKeyIndexGroupIterator& rhs) const {
        return _ordinal == rhs._ordinal && _decoder_group == rhs._decoder_group;
    }

    ssize_t operator-(const ShortKeyIndexGroupIterator& rhs) const { return _ordinal - rhs._ordinal; }

    bool valid() const;

    Slice operator*() const;

    ssize_t ordinal() const { return _ordinal; }

private:
    const ShortKeyIndexDecoderGroup* _decoder_group;
    ssize_t _ordinal;
};

// ShortKeyIndexDecoderGroup contains decoders for each segment of SegmentGroup.
class ShortKeyIndexDecoderGroup {
public:
    explicit ShortKeyIndexDecoderGroup(const std::vector<SegmentSharedPtr>& segments);

    ShortKeyIndexGroupIterator begin() const;
    ShortKeyIndexGroupIterator end() const;
    ShortKeyIndexGroupIterator back() const;

    ShortKeyIndexGroupIterator lower_bound(const Slice& key) const;
    ShortKeyIndexGroupIterator upper_bound(const Slice& key) const;

    Slice key(ssize_t ordinal) const;

    ssize_t num_blocks() const { return _decoder_start_ordinals.back(); }

private:
    template <bool lower_bound>
    ShortKeyIndexGroupIterator _seek(const Slice& key) const;
    void _find_position(ssize_t ordinal, ssize_t* decoder_id, ssize_t* block_id_in_decoder) const;

private:
    // The short keys among decoders are increasing.
    std::vector<const ShortKeyIndexDecoder*> _sk_index_decoders;
    // _decoder_start_ordinals[i] represents the start ordinal of _sk_index_decoder[i].
    // _decoder_start_ordinals[_sk_index_decoders.size()] represents the total number of rows among decoders.
    std::vector<ssize_t> _decoder_start_ordinals;
};

// SegmentGroup contains multiple non-overlapping segments incremented by the key columns.
// It is used to perform binary search on the short keys of all the segments.
class SegmentGroup {
public:
    SegmentGroup(std::vector<SegmentSharedPtr>&& segments);

    ShortKeyIndexGroupIterator lower_bound(const Slice& key) const { return _decoder_group.lower_bound(key); }

    ShortKeyIndexGroupIterator upper_bound(const Slice& key) const { return _decoder_group.upper_bound(key); }

    ShortKeyIndexGroupIterator begin() const { return _decoder_group.begin(); }

    ShortKeyIndexGroupIterator end() const { return _decoder_group.end(); }

    ShortKeyIndexGroupIterator back() const { return _decoder_group.back(); }

    ssize_t num_blocks() const { return _decoder_group.num_blocks(); }

    uint32_t num_rows_per_block() const {
        if (_segments.empty()) {
            return 0;
        }
        return _segments[0]->num_rows_per_block();
    }

    size_t num_short_keys() const {
        if (_segments.empty()) {
            return 0;
        }
        return _segments[0]->num_short_keys();
    }

private:
    std::vector<SegmentSharedPtr> _segments;
    ShortKeyIndexDecoderGroup _decoder_group;
};

} // namespace starrocks
