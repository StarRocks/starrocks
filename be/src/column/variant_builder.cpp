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

            // Read the [offset, next_offset) byte range for this field's value.
            const uint32_t offset = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.offset_start_offset + i * info.offset_size, info.offset_size);
            const uint32_t next_offset = VariantUtil::read_little_endian_unsigned32(
                    raw.data() + info.offset_start_offset + (i + 1) * info.offset_size, info.offset_size);
            if (next_offset < offset || info.data_start_offset + next_offset > raw.size()) {
                return Status::VariantError("Invalid variant object field offset");
            }

            VariantNode child;
            RETURN_IF_ERROR(decode_variant_to_node(
                    metadata, VariantValue(raw.substr(info.data_start_offset + offset, next_offset - offset)), &child));
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
