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

#include "storage/rowset/segment_group.h"

namespace starrocks {

/// ShortKeyIndexGroupIterator
ShortKeyIndexGroupIterator::ShortKeyIndexGroupIterator(const ShortKeyIndexDecoderGroup* decoder_group, ssize_t ordinal)
        : _decoder_group(decoder_group), _ordinal(ordinal) {}

bool ShortKeyIndexGroupIterator::valid() const {
    return _ordinal >= 0 && _ordinal < _decoder_group->num_blocks();
}

Slice ShortKeyIndexGroupIterator::operator*() const {
    DCHECK(valid());
    return _decoder_group->key(_ordinal);
}

/// ShortKeyIndexDecoderGroup
ShortKeyIndexDecoderGroup::ShortKeyIndexDecoderGroup(const std::vector<SegmentSharedPtr>& segments) {
    size_t num_decoders = segments.size();

    _sk_index_decoders.reserve(num_decoders);
    for (const auto& segment : segments) {
        DCHECK(segment->has_loaded_index());
        _sk_index_decoders.emplace_back(segment->decoder());
    }

    _decoder_start_ordinals.reserve(num_decoders + 1);
    ssize_t num_total_items = 0;
    _decoder_start_ordinals.emplace_back(num_total_items);
    for (auto* decoder : _sk_index_decoders) {
        num_total_items += decoder->num_items();
        _decoder_start_ordinals.emplace_back(num_total_items);
    }
}

ShortKeyIndexGroupIterator ShortKeyIndexDecoderGroup::begin() const {
    return {this, 0};
}

ShortKeyIndexGroupIterator ShortKeyIndexDecoderGroup::end() const {
    return {this, num_blocks()};
}

ShortKeyIndexGroupIterator ShortKeyIndexDecoderGroup::back() const {
    return {this, num_blocks() - 1};
}

template <bool is_lower_bound>
ShortKeyIndexGroupIterator ShortKeyIndexDecoderGroup::_seek(const Slice& key) const {
    auto comparator = [](const Slice& lhs, const Slice& rhs) { return lhs.compare(rhs) < 0; };
    if constexpr (is_lower_bound) {
        return std::lower_bound(begin(), end(), key, comparator);
    } else {
        return std::upper_bound(begin(), end(), key, comparator);
    }
}

ShortKeyIndexGroupIterator ShortKeyIndexDecoderGroup::lower_bound(const Slice& key) const {
    return _seek<true>(key);
}

ShortKeyIndexGroupIterator ShortKeyIndexDecoderGroup::upper_bound(const Slice& key) const {
    return _seek<false>(key);
}

Slice ShortKeyIndexDecoderGroup::key(ssize_t ordinal) const {
    ssize_t decoder_id;
    ssize_t block_id_in_decoder;
    _find_position(ordinal, &decoder_id, &block_id_in_decoder);

    return _sk_index_decoders[size_t(decoder_id)]->key(block_id_in_decoder);
}

void ShortKeyIndexDecoderGroup::_find_position(ssize_t ordinal, ssize_t* decoder_id,
                                               ssize_t* block_id_in_decoder) const {
    auto it = std::upper_bound(_decoder_start_ordinals.begin(), _decoder_start_ordinals.end(), ordinal);

    *decoder_id = it - _decoder_start_ordinals.begin() - 1;
    if (*decoder_id < _sk_index_decoders.size()) {
        *block_id_in_decoder = ordinal - _decoder_start_ordinals[size_t(*decoder_id)];
    } else {
        *block_id_in_decoder = 0;
    }
}

/// SegmentGroup
SegmentGroup::SegmentGroup(std::vector<SegmentSharedPtr>&& segments)
        : _segments(std::move(segments)), _decoder_group(_segments) {}

} // namespace starrocks
