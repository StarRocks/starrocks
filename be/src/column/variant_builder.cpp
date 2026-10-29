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

#include "column/variant_builder.h"

#include <algorithm>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/string/utf8_check.h"
#include "column/variant_encoder.h"

namespace starrocks {

enum class NodeKind : uint8_t {
    kNull = 0,
    kScalar = 1,
    kObject = 2,
    kArray = 3,
};

struct StringHash {
    using is_transparent = void;
    size_t operator()(std::string_view sv) const { return std::hash<std::string_view>{}(sv); }
    size_t operator()(const std::string& s) const { return std::hash<std::string_view>{}(s); }
};

struct VariantNode {
    NodeKind kind = NodeKind::kNull;
    std::string scalar_raw;
    std::vector<std::pair<std::string, std::unique_ptr<VariantNode>>> fields;
    std::unordered_map<std::string, size_t, StringHash, std::equal_to<>> field_index;
    std::vector<std::unique_ptr<VariantNode>> elements;

    void set_null() {
        _clear_data();
        kind = NodeKind::kNull;
    }
    void set_scalar(std::string&& raw) {
        _clear_data();
        kind = NodeKind::kScalar;
        scalar_raw = std::move(raw);
    }
    void set_object() {
        if (kind == NodeKind::kObject) return;
        _clear_data();
        kind = NodeKind::kObject;
    }
    void set_array() {
        if (kind == NodeKind::kArray) return;
        _clear_data();
        kind = NodeKind::kArray;
    }

private:
    void _clear_data() {
        scalar_raw.clear();
        fields.clear();
        field_index.clear();
        elements.clear();
    }

public:
    VariantNode* find_or_insert_field(std::string_view key) {
        auto it = field_index.find(key);
        if (it != field_index.end()) {
            return fields[it->second].second.get();
        }
        const size_t idx = fields.size();
        fields.emplace_back(std::string(key), std::make_unique<VariantNode>());
        field_index.emplace(fields.back().first, idx);
        return fields.back().second.get();
    }
};

// Recursively decode a VariantValue binary blob into a mutable VariantNode tree.
// metadata provides the dictionary for resolving field_id -> key string.
// Scalars are stored as raw binary (preserving original encoding); objects and arrays
// are decoded recursively so overlays can be applied by key/index before re-encoding.
static Status decode_variant_to_node(const VariantMetadata& metadata, const VariantValue& value, VariantNode* node) {
    if (node == nullptr) {
        return Status::InvalidArgument("decode target node is null");
    }
    const VariantType type = value.type();

    if (type == VariantType::NULL_TYPE) {
        node->set_null();
        return Status::OK();
    }

    if (type == VariantType::OBJECT) {
        ASSIGN_OR_RETURN(const auto info, value.get_object_info());
        const std::string_view raw = value.raw();
        node->set_object();
        node->fields.reserve(info.num_elements);
        node->field_index.reserve(info.num_elements);
        for (uint32_t i = 0; i < info.num_elements; ++i) {
            // Resolve field_id -> key string via metadata dictionary.
            const uint32_t field_id = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.id_start_offset + i * info.id_size, info.id_size);
            ASSIGN_OR_RETURN(const auto key, metadata.get_key(field_id));

            // Field-name order and physical payload order can differ in an OBJECT.
            ASSIGN_OR_RETURN(const auto field, value.get_object_field_at_index(i));
            VariantNode child;
            RETURN_IF_ERROR(decode_variant_to_node(metadata, field, &child));
            const size_t idx = node->fields.size();
            node->fields.emplace_back(std::string(key), std::make_unique<VariantNode>(std::move(child)));
            node->field_index.emplace(node->fields.back().first, idx);
        }
        return Status::OK();
    }

    if (type == VariantType::ARRAY) {
        ASSIGN_OR_RETURN(const auto info, value.get_array_info());
        const std::string_view raw = value.raw();
        node->set_array();
        node->elements.resize(info.num_elements);
        for (uint32_t i = 0; i < info.num_elements; ++i) {
            // Read the [offset, next_offset) byte range for this element's value.
            const uint32_t offset = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.offset_start_offset + i * info.offset_size, info.offset_size);
            const uint32_t next_offset = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.offset_start_offset + (i + 1) * info.offset_size, info.offset_size);
            if (next_offset < offset || info.data_start_offset + next_offset > raw.size()) {
                return Status::VariantError("Invalid variant array element offset");
            }
            auto child = std::make_unique<VariantNode>();
            RETURN_IF_ERROR(decode_variant_to_node(
                    metadata, VariantValue(raw.substr(info.data_start_offset + offset, next_offset - offset)),
                    child.get()));
            node->elements[i] = std::move(child);
        }
        return Status::OK();
    }

    // Scalar: store raw bytes as-is; re-encoding will copy them directly.
    node->set_scalar(std::string(value.raw()));
    return Status::OK();
}

static void collect_object_keys(const VariantNode& node, std::unordered_set<std::string>* keys) {
    if (node.kind == NodeKind::kObject) {
        for (const auto& [key, child] : node.fields) {
            keys->emplace(key);
            collect_object_keys(*child, keys);
        }
        return;
    }
    if (node.kind == NodeKind::kArray) {
        for (const auto& element : node.elements) {
            if (element != nullptr) {
                collect_object_keys(*element, keys);
            }
        }
    }
}

static Status encode_node_value(const VariantNode& node, const std::unordered_map<std::string, uint32_t>& dict_indexes,
                                std::string* out) {
    if (node.kind == NodeKind::kNull) {
        VariantEncoder::append_null_value(out);
        return Status::OK();
    }
    if (node.kind == NodeKind::kScalar) {
        out->append(node.scalar_raw.data(), node.scalar_raw.size());
        return Status::OK();
    }
    if (node.kind == NodeKind::kArray) {
        std::string payload;
        std::vector<uint32_t> end_offsets;
        end_offsets.reserve(node.elements.size());
        for (const auto& element : node.elements) {
            if (element == nullptr) {
                VariantEncoder::append_null_value(&payload);
            } else {
                RETURN_IF_ERROR(encode_node_value(*element, dict_indexes, &payload));
            }
            end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
        }
        VariantEncoder::append_array_container(out, end_offsets, payload);
        return Status::OK();
    }

    std::vector<std::pair<uint32_t, const VariantNode*>> fields;
    fields.reserve(node.fields.size());
    for (const auto& [key, child] : node.fields) {
        auto it = dict_indexes.find(key);
        if (it == dict_indexes.end()) {
            return Status::InvalidArgument("variant mutable builder miss key index: " + key);
        }
        fields.emplace_back(it->second, child.get());
    }
    std::sort(fields.begin(), fields.end(), [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    std::string payload;
    std::vector<uint32_t> field_ids;
    std::vector<uint32_t> end_offsets;
    field_ids.reserve(fields.size());
    end_offsets.reserve(fields.size());
    for (const auto& [field_id, child] : fields) {
        RETURN_IF_ERROR(encode_node_value(*child, dict_indexes, &payload));
        field_ids.emplace_back(field_id);
        end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
    }
    VariantEncoder::append_object_container(out, field_ids, end_offsets, payload);
    return Status::OK();
}

// Apply an overlay VariantNode at the position described by path within root.
// An empty path replaces the root entirely.
// Only object-key segments are allowed; array segments are rejected.
static Status apply_overlay(VariantNode* root, const VariantPath& path, VariantNode&& overlay) {
    if (root == nullptr) {
        return Status::InvalidArgument("overlay root is null");
    }
    if (path.empty()) {
        *root = std::move(overlay);
        return Status::OK();
    }

    VariantNode* current = root;
    for (size_t i = 0; i < path.segments.size(); ++i) {
        const VariantSegment& seg = path.segments[i];
        if (!seg.is_object()) {
            return Status::InvalidArgument("VariantBuilder overlay path must not contain array segments");
        }
        const bool last = i + 1 == path.segments.size();
        current->set_object();
        VariantNode* child = current->find_or_insert_field(seg.get_key());
        if (last) {
            *child = std::move(overlay);
        } else {
            current = child;
        }
    }
    return Status::OK();
}

namespace {

struct StrictVariantBudget {
    size_t work = VariantBuilder::kMaxStrictWork;
    size_t bytes = VariantRowValue::kMaxVariantSize;

    Status consume(size_t count, size_t size) {
        if (count > work || size > bytes) {
            return Status::InvalidArgument("Strict VARIANT materialization work/byte limit exceeded");
        }
        work -= count;
        bytes -= size;
        return Status::OK();
    }
};

uint32_t read_variant_uint(std::string_view raw, size_t offset, uint8_t width) {
    return VariantUtil::read_little_endian_unsigned32(raw.data() + offset, width);
}

StatusOr<std::vector<std::string_view>> strict_metadata_keys(std::string_view raw, StrictVariantBudget* budget) {
    RETURN_IF_ERROR(budget->consume(0, raw.size()));
    RETURN_IF_ERROR(VariantRowValue::validate_metadata(raw));
    const uint8_t header = static_cast<uint8_t>(raw[0]);
    if ((header & 0x20) != 0) {
        return Status::InvalidArgument("Reserved VARIANT metadata header bit");
    }
    const uint8_t width = (header >> 6) + 1;
    if (raw.size() < size_t{1} + width) {
        return Status::InvalidArgument("Truncated VARIANT dictionary size");
    }
    const uint32_t count = read_variant_uint(raw, 1, width);
    RETURN_IF_ERROR(budget->consume(count, 0));
    const size_t data_start = 1 + size_t{width} * (size_t{count} + 2);
    if (data_start > raw.size()) {
        return Status::InvalidArgument("Truncated VARIANT dictionary offsets");
    }
    if (read_variant_uint(raw, 1 + width, width) != 0 ||
        read_variant_uint(raw, 1 + size_t{width} * (size_t{count} + 1), width) != raw.size() - data_start) {
        return Status::InvalidArgument("Invalid VARIANT dictionary extent");
    }
    std::vector<std::string_view> keys;
    keys.reserve(count);
    for (uint32_t i = 0; i < count; ++i) {
        const uint32_t begin = read_variant_uint(raw, 1 + size_t{width} * (size_t{i} + 1), width);
        const uint32_t end = read_variant_uint(raw, 1 + size_t{width} * (size_t{i} + 2), width);
        if (end < begin || end > raw.size() - data_start) {
            return Status::InvalidArgument("Invalid VARIANT dictionary offset");
        }
        const auto key = raw.substr(data_start + begin, end - begin);
        if (!validate_utf8(key.data(), key.size()) || ((header & 0x10) != 0 && !keys.empty() && keys.back() >= key)) {
            return Status::InvalidArgument("Invalid VARIANT dictionary string/order");
        }
        keys.emplace_back(key);
    }
    return keys;
}

// Validate physical child extents, not next-field offsets: OBJECT field order is lexical,
// whereas ARRAY elements must be adjacent in index order. Scalars remain opaque binary.
Status strict_variant_value(std::string_view raw, const std::vector<std::string_view>& keys, size_t depth,
                            StrictVariantBudget* budget) {
    if (depth > VariantBuilder::kMaxStrictDepth) {
        return Status::InvalidArgument("Strict VARIANT materialization nesting limit exceeded");
    }
    RETURN_IF_ERROR(budget->consume(1, 0));
    if (raw.empty()) {
        return Status::InvalidArgument("Empty VARIANT value");
    }
    const uint8_t header = static_cast<uint8_t>(raw[0]);
    const uint8_t basic = header & 3;
    const uint8_t info = header >> 2;
    if (basic == 1) {
        if (raw.size() != size_t{info} + 1 || !validate_utf8(raw.data() + 1, info)) {
            return Status::InvalidArgument("Invalid VARIANT short string");
        }
        return Status::OK();
    }
    if (basic == 0) {
        size_t expected;
        switch (static_cast<VariantType>(info)) {
        case VariantType::NULL_TYPE:
        case VariantType::BOOLEAN_TRUE:
        case VariantType::BOOLEAN_FALSE:
            expected = 1;
            break;
        case VariantType::INT8:
            expected = 2;
            break;
        case VariantType::INT16:
            expected = 3;
            break;
        case VariantType::INT32:
        case VariantType::DATE:
        case VariantType::FLOAT:
            expected = 5;
            break;
        case VariantType::INT64:
        case VariantType::DOUBLE:
        case VariantType::TIMESTAMP_TZ:
        case VariantType::TIMESTAMP_NTZ:
        case VariantType::TIME_NTZ:
        case VariantType::TIMESTAMP_TZ_NANOS:
        case VariantType::TIMESTAMP_NTZ_NANOS:
            expected = 9;
            break;
        case VariantType::DECIMAL4:
        case VariantType::DECIMAL8:
        case VariantType::DECIMAL16: {
            const uint8_t precision = info == 8 ? 9 : (info == 9 ? 18 : 38);
            expected = info == 8 ? 6 : (info == 9 ? 10 : 18);
            if (raw.size() < 2 || static_cast<uint8_t>(raw[1]) > precision) {
                return Status::InvalidArgument("Invalid VARIANT decimal scale");
            }
            // Do not constrain the integer bytes to decimal(38): LARGEINT uses the full int128 range.
            break;
        }
        case VariantType::BINARY:
        case VariantType::STRING:
            if (raw.size() < 5 || read_variant_uint(raw, 1, 4) != raw.size() - 5 ||
                (info == static_cast<uint8_t>(VariantType::STRING) && !validate_utf8(raw.data() + 5, raw.size() - 5))) {
                return Status::InvalidArgument("Invalid VARIANT binary/string extent");
            }
            return Status::OK();
        case VariantType::UUID:
            expected = 17;
            break;
        default:
            return Status::InvalidArgument("Unknown VARIANT primitive type");
        }
        return raw.size() == expected ? Status::OK() : Status::InvalidArgument("Invalid VARIANT primitive extent");
    }

    const bool object = basic == 2;
    if ((object && (info & 0x20) != 0) || (!object && (info & 0x38) != 0)) {
        return Status::InvalidArgument("Reserved VARIANT container header bits");
    }
    uint32_t count;
    size_t offsets_start;
    size_t data_start;
    uint8_t width;
    uint8_t id_width = 0;
    size_t ids_start = 0;
    const VariantValue value(raw);
    if (object) {
        ASSIGN_OR_RETURN(const auto shape, value.get_object_info());
        count = shape.num_elements;
        offsets_start = shape.offset_start_offset;
        data_start = shape.data_start_offset;
        width = shape.offset_size;
        id_width = shape.id_size;
        ids_start = shape.id_start_offset;
    } else {
        ASSIGN_OR_RETURN(const auto shape, value.get_array_info());
        count = shape.num_elements;
        offsets_start = shape.offset_start_offset;
        data_start = shape.data_start_offset;
        width = shape.offset_size;
    }
    if (count > budget->work) {
        return Status::InvalidArgument("Strict VARIANT container work limit exceeded");
    }
    const uint32_t total = read_variant_uint(raw, offsets_start + size_t{count} * width, width);
    if (total != raw.size() - data_start) {
        return Status::InvalidArgument("Invalid VARIANT container extent");
    }
    std::vector<uint32_t> offsets;
    offsets.reserve(size_t{count} + 1);
    std::string_view previous_key;
    for (uint32_t i = 0; i < count; ++i) {
        if (object) {
            const uint32_t id = read_variant_uint(raw, ids_start + size_t{i} * id_width, id_width);
            if (id >= keys.size() || (i > 0 && previous_key >= keys[id])) {
                return Status::InvalidArgument("Invalid VARIANT object field id/order");
            }
            previous_key = keys[id];
        }
        const uint32_t offset = read_variant_uint(raw, offsets_start + size_t{i} * width, width);
        if (offset >= total || (!object && !offsets.empty() && offsets.back() >= offset)) {
            return Status::InvalidArgument("Invalid VARIANT child offset");
        }
        offsets.emplace_back(offset);
    }
    if (object) {
        std::sort(offsets.begin(), offsets.end());
    }
    if ((count == 0 && total != 0) || (count > 0 && offsets.front() != 0)) {
        return Status::InvalidArgument("Invalid VARIANT first payload offset");
    }
    offsets.emplace_back(total);
    for (uint32_t i = 0; i < count; ++i) {
        if (offsets[i] >= offsets[i + 1]) {
            return Status::InvalidArgument("Overlapping VARIANT payload offsets");
        }
        RETURN_IF_ERROR(strict_variant_value(raw.substr(data_start + offsets[i], offsets[i + 1] - offsets[i]), keys,
                                             depth + 1, budget));
    }
    return Status::OK();
}

Status strict_variant_row(std::string_view metadata, std::string_view value, size_t depth,
                          StrictVariantBudget* budget) {
    ASSIGN_OR_RETURN(const auto keys, strict_metadata_keys(metadata, budget));
    RETURN_IF_ERROR(budget->consume(0, value.size()));
    return strict_variant_value(value, keys, depth, budget);
}

} // namespace

Status VariantBuilder::validate_metadata_strict(std::string_view metadata) {
    StrictVariantBudget budget;
    return strict_metadata_keys(metadata, &budget).status();
}

Status VariantBuilder::validate_row_strict(std::string_view metadata, std::string_view value) {
    StrictVariantBudget budget;
    return strict_variant_row(metadata, value, 0, &budget);
}

StatusOr<VariantRowValue> VariantBuilder::build_row_from_overlays_strict(std::optional<VariantRowRef> base,
                                                                         std::vector<Overlay> overlays) {
    if (!base.has_value() && overlays.empty()) {
        return Status::InvalidArgument("Missing VARIANT residual and effective overlays");
    }
    StrictVariantBudget budget;
    if (base.has_value()) {
        RETURN_IF_ERROR(strict_variant_row(base->get_metadata().raw(), base->get_value().raw(), 0, &budget));
    }
    for (const auto& overlay : overlays) {
        if (overlay.path.segments.size() > kMaxStrictDepth) {
            return Status::InvalidArgument("Strict VARIANT overlay path nesting limit exceeded");
        }
        for (const auto& segment : overlay.path.segments) {
            if (!segment.is_object() || !validate_utf8(segment.get_key().data(), segment.get_key().size())) {
                return Status::InvalidArgument("Invalid strict VARIANT overlay object path");
            }
            RETURN_IF_ERROR(budget.consume(1, segment.get_key().size()));
        }
        RETURN_IF_ERROR(strict_variant_row(overlay.value.get_metadata().raw(), overlay.value.get_value().raw(),
                                           overlay.path.segments.size(), &budget));
    }
    return build_row_from_overlays(std::move(base), std::move(overlays));
}

Status VariantBuilder::set_overlays(std::vector<Overlay>&& overlays) {
    _overlays = std::move(overlays);
    return Status::OK();
}

StatusOr<VariantRowValue> VariantBuilder::build_row_from_overlays(std::optional<VariantRowRef> base,
                                                                  std::vector<Overlay> overlays) {
    VariantBuilder builder(base.has_value() ? &base.value() : nullptr);
    RETURN_IF_ERROR(builder.set_overlays(std::move(overlays)));
    return builder.build();
}

StatusOr<VariantRowValue> VariantBuilder::build() const {
    VariantNode root;
    bool has_content = false;

    if (_base.has_value()) {
        RETURN_IF_ERROR(decode_variant_to_node(_base->get_metadata(), _base->get_value(), &root));
        has_content = true;
    }

    for (const auto& overlay : _overlays) {
        VariantNode overlay_node;
        RETURN_IF_ERROR(decode_variant_to_node(overlay.value.get_metadata(), overlay.value.get_value(), &overlay_node));
        RETURN_IF_ERROR(apply_overlay(&root, overlay.path, std::move(overlay_node)));
        has_content = true;
    }

    if (!has_content) {
        return VariantRowValue::from_null();
    }

    std::unordered_set<std::string> keys;
    collect_object_keys(root, &keys);
    std::unordered_map<std::string, uint32_t> key_to_id;
    ASSIGN_OR_RETURN(auto metadata, VariantEncoder::build_variant_metadata(keys, &key_to_id));
    std::string value;
    RETURN_IF_ERROR(encode_node_value(root, key_to_id, &value));
    return VariantRowValue::create(metadata, value);
}

StatusOr<VariantRowValue> VariantArrayBuilder::build() const {
    // 1. Decode each element into a VariantNode tree.
    std::vector<VariantNode> nodes(_elements.size());
    for (size_t i = 0; i < _elements.size(); ++i) {
        if (!_elements[i].has_value()) {
            nodes[i].set_null();
        } else {
            RETURN_IF_ERROR(decode_variant_to_node(_elements[i]->get_metadata(), _elements[i]->get_value(), &nodes[i]));
        }
    }

    // 2. Collect all object keys across elements for a unified metadata dictionary.
    std::unordered_set<std::string> keys;
    for (const auto& node : nodes) {
        collect_object_keys(node, &keys);
    }

    // 3. Build unified metadata.
    std::unordered_map<std::string, uint32_t> key_to_id;
    ASSIGN_OR_RETURN(auto metadata, VariantEncoder::build_variant_metadata(keys, &key_to_id));

    // 4. Re-encode each element under the unified dictionary.
    std::string payload;
    std::vector<uint32_t> end_offsets;
    end_offsets.reserve(nodes.size());
    for (const auto& node : nodes) {
        RETURN_IF_ERROR(encode_node_value(node, key_to_id, &payload));
        end_offsets.emplace_back(static_cast<uint32_t>(payload.size()));
    }

    // 5. Assemble the array container.
    std::string value;
    VariantEncoder::append_array_container(&value, end_offsets, payload);
    return VariantRowValue::create(metadata, value);
}

} // namespace starrocks
