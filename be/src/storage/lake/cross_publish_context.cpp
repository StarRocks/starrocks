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

#include "storage/lake/cross_publish_context.h"

#include "column/binary_column.h"
#include "column/chunk.h"
#include "runtime/current_thread.h"
#include "storage/chunk_helper.h"
#include "storage_primitive/primary_key_encoder.h"

namespace starrocks::lake {

namespace {

bool carries_shared_segment(const RowsetMetadataPB& rowset) {
    for (const auto& segment_meta : rowset.segment_metas()) {
        if (segment_meta.shared()) {
            return true;
        }
    }
    return false;
}

} // namespace

StatusOr<CrossPublishRowSelectorPtr> CrossPublishRowSelector::create_if_needed(const TabletMetadataPB& metadata,
                                                                               const TabletSchemaCSPtr& tablet_schema,
                                                                               const RowsetMetadataPB& rowset) {
    if (tablet_schema == nullptr || tablet_schema->keys_type() != KeysType::PRIMARY_KEYS) {
        return nullptr;
    }
    if (!metadata.has_range() || !carries_shared_segment(rowset)) {
        return nullptr;
    }
    ASSIGN_OR_RETURN(auto encoding_type, tablet_schema->primary_key_encoding_type_or_error());
    // select_encoded compares encoded keys bytewise against the encoded bounds, which only follows
    // key order under the order-preserving big-endian encoding. Range distribution requires V2, so
    // anything else here is a tablet that cannot be split in the first place.
    if (encoding_type != PrimaryKeyEncodingType::PK_ENCODING_TYPE_V2) {
        return nullptr;
    }

    auto selector = std::make_unique<CrossPublishRowSelector>();
    ASSIGN_OR_RETURN(selector->_seek_range,
                     TabletRangeHelper::create_sst_seek_range_from(metadata.range(), tablet_schema));
    // Both bounds empty is (-inf, +inf): this tablet owns everything, so there is nothing to select
    // and the caller should keep paying nothing. Mirrors SeekRange::all_range()'s short-circuit in
    // SegmentIterator::_apply_tablet_range.
    if (selector->_seek_range.seek_key.empty() && selector->_seek_range.stop_key.empty()) {
        return nullptr;
    }

    // The range is in primary-key space whatever the segments are ordered by, so the schema is the
    // key columns in key order -- the same one RowsetUpdateState::prepare builds for its upserts.
    std::vector<ColumnId> pk_columns;
    pk_columns.reserve(tablet_schema->num_key_columns());
    for (size_t i = 0; i < tablet_schema->num_key_columns(); i++) {
        pk_columns.push_back(static_cast<ColumnId>(i));
    }
    selector->_pkey_schema = ChunkHelper::convert_schema(tablet_schema, pk_columns);
    selector->_encoding_type = encoding_type;
    return selector;
}

StatusOr<Filter> CrossPublishRowSelector::select(const Chunk& chunk) const {
    const size_t num_rows = chunk.num_rows();
    if (num_rows == 0) {
        return Filter{};
    }
    // Local, not a member: see the note on the declaration -- a rowset's segments load concurrently
    // and share this selector.
    MutableColumnPtr encoded_keys;
    RETURN_IF_ERROR(PrimaryKeyEncoder::create_column(_pkey_schema, &encoded_keys, _encoding_type));
    // A wide or long primary key can make this allocation fail. Publish must see that as a Status it
    // can retry, not as an exception unwinding out of the worker -- same reason
    // SegmentPKIterator::encoded_pk_column wraps its encode.
    TRY_CATCH_BAD_ALLOC(
            PrimaryKeyEncoder::encode(_pkey_schema, chunk, 0, num_rows, encoded_keys.get(), _encoding_type));
    return select_encoded(*encoded_keys);
}

StatusOr<Filter> CrossPublishRowSelector::select_encoded(const Column& encoded_keys) const {
    const size_t num_rows = encoded_keys.size();
    if (num_rows == 0) {
        return Filter{};
    }
    RETURN_ERROR_IF_FALSE(encoded_keys.is_binary(), "V2-encoded primary key must be binary");
    const auto& binary_keys = down_cast<const BinaryColumn&>(encoded_keys);

    const Slice seek_key(_seek_range.seek_key);
    const Slice stop_key(_seek_range.stop_key);
    const bool has_seek = !_seek_range.seek_key.empty();
    const bool has_stop = !_seek_range.stop_key.empty();

    Filter selection;
    TRY_CATCH_BAD_ALLOC(selection.assign(num_rows, 1));
    for (size_t i = 0; i < num_rows; ++i) {
        const Slice key = binary_keys.get_slice(i);
        // create_sst_seek_range_from folds the bounds' inclusiveness into the encoding, so the
        // half-open [seek_key, stop_key) test below is the whole of it.
        if (has_seek && key.compare(seek_key) < 0) {
            selection[i] = 0;
        } else if (has_stop && key.compare(stop_key) >= 0) {
            selection[i] = 0;
        }
    }
    return selection;
}

} // namespace starrocks::lake
