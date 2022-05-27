// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/rowset/segment_group.h"

namespace starrocks {

/// ShortKeyIndexGroupIterator
ShortKeyIndexGroupIterator::ShortKeyIndexGroupIterator(const ShortKeyIndexDecoderGroup* decoder_group, ssize_t ordinal)
        : _decoder_group(decoder_group), _ordinal(ordinal) {
    _decoder_group->position(_ordinal, &_decoder_id, &_block_id_in_decoder);
}

bool ShortKeyIndexGroupIterator::valid() const {
    return _ordinal >= 0 && _ordinal < _decoder_group->num_blocks();
}

Slice ShortKeyIndexGroupIterator::operator*() const {
    DCHECK(valid());
    return _decoder_group->key(_decoder_id, _block_id_in_decoder);
}

/// ShortKeyIndexDecoderGroup
ShortKeyIndexDecoderGroup::ShortKeyIndexDecoderGroup(std::vector<const ShortKeyIndexDecoder*> sk_index_decoders)
        : _sk_index_decoders(std::move(sk_index_decoders)) {
    _decoder_start_ordinals.reserve(_sk_index_decoders.size());
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

ShortKeyIndexGroupIterator ShortKeyIndexDecoderGroup::lower_bound(const Slice& key) const {
    return _seek<true>(key);
}

ShortKeyIndexGroupIterator ShortKeyIndexDecoderGroup::upper_bound(const Slice& key) const {
    return _seek<false>(key);
}

void ShortKeyIndexDecoderGroup::position(ssize_t ordinal, ssize_t* decoder_id, ssize_t* block_id_in_decoder) const {
    auto it = std::upper_bound(_decoder_start_ordinals.begin(), _decoder_start_ordinals.end(), ordinal);

    *decoder_id = it - _decoder_start_ordinals.begin() - 1;
    if (*decoder_id < _sk_index_decoders.size()) {
        // 0 1 2, 3 4 5 6, 7
        // 0 3 7
        *block_id_in_decoder = ordinal - _decoder_start_ordinals[size_t(*decoder_id)];
    } else {
        *block_id_in_decoder = 0;
    }
}

Slice ShortKeyIndexDecoderGroup::key(ssize_t ordinal) const {
    ssize_t decoder_id;
    ssize_t block_id_in_decoder;
    position(ordinal, &decoder_id, &block_id_in_decoder);

    return key(decoder_id, block_id_in_decoder);
}

Slice ShortKeyIndexDecoderGroup::key(ssize_t decoder_id, ssize_t block_id_in_decoder) const {
    return _sk_index_decoders[size_t(decoder_id)]->key(block_id_in_decoder);
}

template <bool lower_bound>
ShortKeyIndexGroupIterator ShortKeyIndexDecoderGroup::_seek(const Slice& key) const {
    auto comparator = [](const Slice& lhs, const Slice& rhs) { return lhs.compare(rhs) < 0; };
    if (lower_bound) {
        return std::lower_bound(begin(), end(), key, comparator);
    } else {
        return std::upper_bound(begin(), end(), key, comparator);
    }
}

/// SegmentGroup
SegmentGroup::SegmentGroup(std::vector<SegmentSharedPtr> segments) : _segments(std::move(segments)) {
    std::vector<const ShortKeyIndexDecoder*> sk_index_decoders;
    sk_index_decoders.reserve(_segments.size());
    for (const auto& segment : _segments) {
        sk_index_decoders.emplace_back(segment->decoder());
    }
    _decoder_group = std::make_unique<ShortKeyIndexDecoderGroup>(std::move(sk_index_decoders));
}

} // namespace starrocks
