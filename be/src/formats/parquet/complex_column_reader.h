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

#include <string>
#include <vector>

#include "column/variant_path_parser.h"
#include "formats/parquet/column_reader.h"
#include "scalar_column_reader.h"
#include "stored_column_reader.h"
#include "types/type_descriptor.h"

namespace starrocks::parquet {

class ListColumnReader final : public ColumnReader {
public:
    explicit ListColumnReader(const ParquetField* parquet_field, std::unique_ptr<ColumnReader>&& element_reader)
            : ColumnReader(parquet_field), _element_reader(std::move(element_reader)) {}
    ~ListColumnReader() override = default;

    Status prepare() override { return _element_reader->prepare(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _element_reader->get_levels(def_levels, rep_levels, num_levels);
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        _element_reader->set_need_parse_levels(need_parse_levels);
    }

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {
        _element_reader->collect_column_io_range(ranges, end_offset, types, active);
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {
        _element_reader->select_offset_index(range, rg_first_row);
    }

    ColumnReaderPtr& get_element_reader() { return _element_reader; }

private:
    std::unique_ptr<ColumnReader> _element_reader;
};

class MapColumnReader final : public ColumnReader {
public:
    explicit MapColumnReader(const ParquetField* parquet_field, std::unique_ptr<ColumnReader>&& key_reader,
                             std::unique_ptr<ColumnReader>&& value_reader)
            : ColumnReader(parquet_field), _key_reader(std::move(key_reader)), _value_reader(std::move(value_reader)) {}
    ~MapColumnReader() override = default;

    Status prepare() override {
        // Check must has one valid column reader
        if (_key_reader == nullptr && _value_reader == nullptr) {
            return Status::InternalError("No available subfield column reader in MapColumnReader");
        }

        if (_key_reader != nullptr) {
            RETURN_IF_ERROR(_key_reader->prepare());
        }
        if (_value_reader != nullptr) {
            RETURN_IF_ERROR(_value_reader->prepare());
        }

        return Status::OK();
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        // check _value_reader
        if (_key_reader != nullptr) {
            _key_reader->get_levels(def_levels, rep_levels, num_levels);
        } else if (_value_reader != nullptr) {
            _value_reader->get_levels(def_levels, rep_levels, num_levels);
        } else {
            DCHECK(false) << "Unreachable!";
        }
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        if (_key_reader != nullptr) {
            _key_reader->set_need_parse_levels(need_parse_levels);
        }

        if (_value_reader != nullptr) {
            _value_reader->set_need_parse_levels(need_parse_levels);
        }
    }

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {
        if (_key_reader != nullptr) {
            _key_reader->collect_column_io_range(ranges, end_offset, types, active);
        }
        if (_value_reader != nullptr) {
            _value_reader->collect_column_io_range(ranges, end_offset, types, active);
        }
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {
        if (_key_reader != nullptr) {
            _key_reader->select_offset_index(range, rg_first_row);
        }
        if (_value_reader != nullptr) {
            _value_reader->select_offset_index(range, rg_first_row);
        }
    }

private:
    std::unique_ptr<ColumnReader> _key_reader;
    std::unique_ptr<ColumnReader> _value_reader;
};

class StructColumnReader final : public ColumnReader {
public:
    explicit StructColumnReader(const ParquetField* parquet_field,
                                std::map<std::string, std::unique_ptr<ColumnReader>>&& child_readers)
            : ColumnReader(parquet_field), _child_readers(std::move(child_readers)) {}
    ~StructColumnReader() override = default;

    Status prepare() override {
        if (_child_readers.empty()) {
            return Status::InternalError("No avaliable parquet subfield column reader in StructColumn");
        }

        for (const auto& child : _child_readers) {
            if (child.second != nullptr) {
                RETURN_IF_ERROR(child.second->prepare());
            }
        }

        for (const auto& pair : _child_readers) {
            if (pair.second != nullptr) {
                _def_rep_level_child_reader = &(pair.second);
                return Status::OK();
            }
        }

        return Status::InternalError("No existed parquet subfield column reader in StructColumn");
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    void set_can_lazy_decode(bool can_lazy_decode) override {
        for (const auto& kv : _child_readers) {
            if (kv.second == nullptr) continue;
            kv.second->set_can_lazy_decode(can_lazy_decode);
        }
    }

    // get_levels functions only called by complex type
    // If parent is a struct type, only def_levels has value.
    // If parent is list or map type, def_levels & rep_levels both have value.
    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        for (const auto& pair : _child_readers) {
            // Considering not existed subfield, we will not create its ColumnReader
            // So we should pick up the first existed subfield column reader
            if (pair.second != nullptr) {
                pair.second->get_levels(def_levels, rep_levels, num_levels);
                return;
            }
        }
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        for (const auto& pair : _child_readers) {
            if (pair.second != nullptr) {
                pair.second->set_need_parse_levels(need_parse_levels);
            }
        }
    }

    bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                const std::vector<std::string>& sub_field_path, const size_t& layer) override;

    Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered, const std::vector<std::string>& sub_field_path,
                                              const size_t& layer) override {
        const std::string& sub_field = sub_field_path[layer];
        return _child_readers[sub_field]->rewrite_conjunct_ctxs_to_predicate(is_group_filtered, sub_field_path,
                                                                             layer + 1);
    }

    Status filter_dict_column(ColumnPtr& column, Filter* filter, const std::vector<std::string>& sub_field_path,
                              const size_t& layer) override;

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override;

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {
        for (const auto& pair : _child_readers) {
            if (pair.second != nullptr) {
                pair.second->collect_column_io_range(ranges, end_offset, types, active);
            }
        }
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {
        for (const auto& pair : _child_readers) {
            if (pair.second != nullptr) {
                pair.second->select_offset_index(range, rg_first_row);
            }
        }
    }

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override;

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override;

    StatusOr<bool> row_group_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                          CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                          const uint64_t rg_num_rows) const override;

    ColumnReader* get_child_column_reader(const std::string& subfield) const;

    // retrieve multi level subfield's ColumnReader
    ColumnReader* get_child_column_reader(const std::vector<std::string>& subfields) const;

private:
    Status _rewrite_column_expr_predicate(ObjectPool* pool, const std::vector<const ColumnPredicate*>& src_preds,
                                          std::vector<const ColumnPredicate*>& dst_preds) const;

    StatusOr<ColumnPredicate*> _try_to_rewrite_subfield_expr(ObjectPool* pool, const ColumnPredicate* predicate,
                                                             std::vector<std::string>* subfield_output) const;

    void _handle_null_rows(uint8_t* is_nulls, bool* has_null, size_t num_rows);

    // _children_readers order is the same as TypeDescriptor children order.
    std::map<std::string, ColumnReaderPtr> _child_readers;
    // First non-nullptr child ColumnReader, used to get def & rep levels
    const std::unique_ptr<ColumnReader>* _def_rep_level_child_reader = nullptr;
};

// ShreddedFieldNode holds both schema-time reader setup and batch-local column data.
//
// Schema-time fields (set once at construction, never modified):
//   full_path, parsed_full_path, value_reader, typed_value_reader, typed_value_read_type,
//   scalar_array_layout, array_element_value_reader, kind, children
//
// Batch-local fields (reset per read_range call):
//   value_column, typed_value_column, array_element_value_column
//
// IMPORTANT – vector resize safety:
//   ScalarColumnReader inside value_reader stores a raw `const TypeDescriptor*` pointing
//   to the heap-allocated object owned by typed_value_read_type (unique_ptr). Moving a
//   ShreddedFieldNode transfers the unique_ptr but the heap object address is unchanged,
//   so pointers remain valid after moves. However, the _shredded_fields vector must NOT
//   be resized after any reader is constructed. Fill the vector fully before calling
//   any reader construction that captures these addresses.
struct ShreddedFieldNode {
    enum class Kind : uint8_t { NONE = 0, SCALAR = 1, ARRAY = 2 };

    std::string name;
    std::string full_path;
    VariantPath parsed_full_path;
    std::unique_ptr<ScalarColumnReader> value_reader;
    std::unique_ptr<ColumnReader> typed_value_reader;
    // Heap-allocated to keep a stable address for reader-side pointer capture (ScalarColumnReader holds const TypeDescriptor*).
    std::unique_ptr<TypeDescriptor> typed_value_read_type;
    Kind kind = Kind::NONE;
    std::vector<ShreddedFieldNode> children;
    // ARRAY scalar layout: list.element has both {value, typed_value(scalar)}.
    // In this mode, we read element.value as an additional per-element fallback source.
    bool scalar_array_layout = false;
    std::unique_ptr<ColumnReader> array_element_value_reader;

    // batch-local columns filled during read_range, reset each call.
    ColumnPtr value_column;
    ColumnPtr typed_value_column;
    ColumnPtr array_element_value_column;
};

enum class VariantScalarMaterializeMode : uint8_t {
    KEEP_SCALAR = 0,
    DEMOTE_VARIANT = 1,
    DROP = 2,
};

struct TopBinding {
    enum class Kind : uint8_t { SCALAR = 0, VARIANT = 1 };
    Kind kind = Kind::SCALAR;
    std::string path;
    TypeDescriptor type;
    const ShreddedFieldNode* node = nullptr;
    // Cached parsed form of `path` to avoid re-parsing on every row.
    VariantPath parsed_path;
};

// VariantColumnReader handles the reading of Parquet columns that represent variant types.
// It uses two ScalarColumnReader instances: one for reading metadata (type information)
// and another for reading the actual variant values.
//
// Thread-safety: NOT thread-safe. ShreddedFieldNode::value_column and
// ShreddedFieldNode::typed_value_column are mutated per read_range() call. Concurrent
// calls to read_range() on the same instance are not allowed.
class VariantColumnReader final : public ColumnReader {
private:
    // Top-level variant payload readers and batch-local typed buffer.
    // `metadata` and `value` are required for variant files.
    // `root_typed_value_*` is optional and used when top-level typed_value is non-STRUCT.
    struct VariantTopLevelReaders {
        VariantTopLevelReaders(std::unique_ptr<ScalarColumnReader>&& metadata_reader,
                               std::unique_ptr<ScalarColumnReader>&& value_reader,
                               ColumnReaderPtr&& root_typed_value_reader,
                               std::unique_ptr<TypeDescriptor> root_typed_value_type)
                : metadata_reader(std::move(metadata_reader)),
                  value_reader(std::move(value_reader)),
                  root_typed_value_reader(std::move(root_typed_value_reader)),
                  root_typed_value_type(std::move(root_typed_value_type)) {}

        std::unique_ptr<ScalarColumnReader> metadata_reader;
        std::unique_ptr<ScalarColumnReader> value_reader;
        ColumnReaderPtr root_typed_value_reader;
        std::unique_ptr<TypeDescriptor> root_typed_value_type;
        ColumnPtr root_typed_value_column;
    };

public:
    static VariantScalarMaterializeMode decide_variant_scalar_materialize_mode(const ShreddedFieldNode* node,
                                                                               size_t num_rows);

    static StatusOr<std::optional<VariantRowValue>> build_variant_binding_from_node(size_t row,
                                                                                    const ShreddedFieldNode& node,
                                                                                    std::string_view metadata_raw);

    static Status append_variant_binding_row(size_t row, const TopBinding& binding, std::string_view raw_metadata,
                                             const VariantRowRef& full_row, Column* dst);

    // Constructor that accepts pre-built ScalarColumnReader objects and optional shredded paths.
    // parsed_shredded_paths: exact leaf or array-boundary paths to expose as typed_columns.
    // If empty, no typed_columns optimization is applied (overlay reconstruction still works).
    explicit VariantColumnReader(const ParquetField* parquet_field,
                                 std::unique_ptr<ScalarColumnReader>&& metadata_reader,
                                 std::unique_ptr<ScalarColumnReader>&& value_reader,
                                 std::vector<ShreddedFieldNode>&& shredded_fields,
                                 std::vector<VariantPath> parsed_shredded_paths = {},
                                 ColumnReaderPtr&& root_typed_value_reader = nullptr,
                                 std::unique_ptr<TypeDescriptor> root_typed_value_type = nullptr)
            : ColumnReader(parquet_field),
              _top_level(std::move(metadata_reader), std::move(value_reader), std::move(root_typed_value_reader),
                         std::move(root_typed_value_type)),
              _shredded_fields(std::move(shredded_fields)),
              _requested_shredded_paths(std::move(parsed_shredded_paths)) {
        // Both readers must be non-null for VariantColumnReader to function correctly
        DCHECK(_top_level.metadata_reader != nullptr) << "VariantColumnReader: metadata reader cannot be null";
        DCHECK(_top_level.value_reader != nullptr) << "VariantColumnReader: value reader cannot be null";
    }

    ~VariantColumnReader() override = default;

    Status prepare() override;

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override;

    void set_need_parse_levels(bool need_parse_levels) override;

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override;

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override;

    // Returns the typed_value ColumnReader for the given parsed variant path, only if the node
    // is a SCALAR kind and has a non-binary type (i.e. safe for predicate/zone-map filtering).
    // Returns nullptr if the path is absent, the node is ARRAY/NONE kind, or the physical type
    // is BINARY/VARBINARY (whose Parquet min/max reflects byte-order, not semantic value order).
    // Array segments are not supported (shredded paths are object-key-only).
    const ColumnReader* filterable_typed_value_reader_for_path(const VariantPath& path) const;
    // Returns the typed_value ColumnReader (mutable) for the given path if the node is SCALAR kind.
    // Unlike filterable_typed_value_reader_for_path, this does NOT exclude BINARY/VARBINARY types;
    // it is used for dict-filter promotion where the type check is delegated to the reader itself.
    // Returns nullptr if the path is absent or the node is not SCALAR kind.
    ColumnReader* scalar_typed_value_reader_for_path(const VariantPath& path);
    // Returns true if all requested shredded paths are scalar typed-value leaves and the base
    // payload (metadata + value columns) can be skipped entirely for this row group.
    bool skip_base_payload() const { return _skip_base_payload; }
    // Returns the typed_value read type for the given parsed variant path.
    // The returned descriptor reflects the shredded leaf's physical typed_value encoding,
    // not the virtual slot's target type.
    const TypeDescriptor* typed_value_read_type_for_path(const VariantPath& path) const;
    // Variant shredding only allows data skipping on typed_value statistics when the paired
    // fallback value column is null for the entire row group. If null_count is absent,
    // conservatively return false.
    bool fallback_values_all_null_in_row_group_for_path(const VariantPath& path, uint64_t rg_num_rows) const;

private:
    struct TopLevelSkipFlags {
        bool skip_payload = false;
        bool skip_metadata = false;
    };

    TopLevelSkipFlags _compute_top_level_skip_flags() const;

    // Fast-path read when _skip_base_payload is true.
    // Returns true if the fast path handled the read completely.
    // Returns false if fallback rows were detected; shredded fields are already populated and the
    // caller should run the normal per-row path without re-reading shredded fields.
    StatusOr<bool> _read_range_skip_base_payload(const Range<uint64_t>& range, const Filter* filter,
                                                 VariantColumn* variant_column, NullableColumn* nullable_column);

    VariantTopLevelReaders _top_level;
    std::vector<ShreddedFieldNode> _shredded_fields;
    std::vector<VariantPath> _requested_shredded_paths;
    // True when every requested path maps to a SCALAR typed_value node in the shredded schema.
    // In that case metadata/value base payload columns are never needed and can be skipped
    // entirely (IO range, offset-index selection, and data read).
    bool _skip_base_payload = false;
    // Cached auto-discovered paths when _requested_shredded_paths is empty (request-all-paths mode).
    // _shredded_fields is fixed after construction, so this only needs to be computed once.
    mutable std::vector<VariantPath> _cached_auto_paths;
    mutable bool _auto_paths_cached = false;
};

// A thin, read-only wrapper ColumnReader that exposes zone-map filtering for a specific
// shredded typed-leaf path within a VariantColumnReader.  It registers no IO ranges and
// does not support actual data reads.  Registered in GroupReader::_column_readers under
// the virtual variant slot id so that PredicateFilterEvaluator can invoke zone-map
// methods on the underlying shredded leaf column's statistics.
class VariantVirtualZoneMapReader final : public ColumnReader {
public:
    VariantVirtualZoneMapReader(VariantColumnReader* source, VariantPath leaf_path);
    VariantVirtualZoneMapReader(VariantColumnReader* source, VariantPath leaf_path, TypeDescriptor virtual_slot_type);
    ~VariantVirtualZoneMapReader() override = default;

    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>&, const Filter*, ColumnPtr&) override {
        return Status::NotSupported("VariantVirtualZoneMapReader does not support read_range");
    }

    void get_levels(level_t**, level_t**, size_t*) override {}

    void set_need_parse_levels(bool) override {}

    // No IO ranges — this reader has no Parquet pages of its own.
    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>*, int64_t*, ColumnIOTypeFlags,
                                 bool) override {}

    void select_offset_index(const SparseRange<uint64_t>&, const uint64_t) override {}

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override;

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override;

    // Delegates bloom-filter evaluation to the shredded typed_value leaf reader.
    // Only equality predicates reach this path; range predicates use zone map instead.
    // Returns false (= don't skip) when the leaf is not shredded or has no bloom filter.
    StatusOr<bool> row_group_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                          CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                          const uint64_t rg_num_rows) const override;

private:
    bool _prepare_delegate_predicates(const std::vector<const ColumnPredicate*>& predicates, ObjectPool* pool,
                                      uint64_t rg_num_rows, const ColumnReader** leaf_reader,
                                      std::vector<const ColumnPredicate*>* rewritten_predicates) const;

    VariantColumnReader* _source;
    VariantPath _leaf_path;
    TypeDescriptor _virtual_slot_type;
};

// A thin read-through proxy for a shredded variant typed_value leaf ColumnReader.
// Used when a virtual variant column is promoted to Phase 2 (with dict filter support),
// bypassing the full VariantColumn construction path.  The underlying reader is non-owning:
// it is owned by the ShreddedFieldNode inside the parent VariantColumnReader.
class VariantTypedValueProxy final : public ColumnReader {
public:
    explicit VariantTypedValueProxy(ColumnReader* reader)
            : ColumnReader(reader->get_column_parquet_field()), _reader(reader) {}

    // No-op: the underlying reader is already prepared by the parent VariantColumnReader.
    Status prepare() override { return Status::OK(); }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override {
        return _reader->read_range(range, filter, dst);
    }

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _reader->get_levels(def_levels, rep_levels, num_levels);
    }

    void set_need_parse_levels(bool need_parse_levels) override { _reader->set_need_parse_levels(need_parse_levels); }

    void set_can_lazy_decode(bool can_lazy_decode) override { _reader->set_can_lazy_decode(can_lazy_decode); }

    bool try_to_use_dict_filter(ExprContext* ctx, bool is_decode_needed, const SlotId slotId,
                                const std::vector<std::string>& sub_field_path, const size_t& layer) override {
        return _reader->try_to_use_dict_filter(ctx, is_decode_needed, slotId, sub_field_path, layer);
    }

    Status rewrite_conjunct_ctxs_to_predicate(bool* is_group_filtered, const std::vector<std::string>& sub_field_path,
                                              const size_t& layer) override {
        return _reader->rewrite_conjunct_ctxs_to_predicate(is_group_filtered, sub_field_path, layer);
    }

    Status filter_dict_column(ColumnPtr& column, Filter* filter, const std::vector<std::string>& sub_field_path,
                              const size_t& layer) override {
        return _reader->filter_dict_column(column, filter, sub_field_path, layer);
    }

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override { return _reader->fill_dst_column(dst, src); }

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOTypeFlags types, bool active) override {
        _reader->collect_column_io_range(ranges, end_offset, types, active);
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {
        _reader->select_offset_index(range, rg_first_row);
    }

    StatusOr<bool> row_group_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                             CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                             const uint64_t rg_num_rows) const override {
        return _reader->row_group_zone_map_filter(predicates, pred_relation, rg_first_row, rg_num_rows);
    }

    StatusOr<bool> page_index_zone_map_filter(const std::vector<const ColumnPredicate*>& predicates,
                                              SparseRange<uint64_t>* row_ranges, CompoundNodeType pred_relation,
                                              const uint64_t rg_first_row, const uint64_t rg_num_rows) override {
        return _reader->page_index_zone_map_filter(predicates, row_ranges, pred_relation, rg_first_row, rg_num_rows);
    }

    StatusOr<bool> row_group_bloom_filter(const std::vector<const ColumnPredicate*>& predicates,
                                          CompoundNodeType pred_relation, const uint64_t rg_first_row,
                                          const uint64_t rg_num_rows) const override {
        return _reader->row_group_bloom_filter(predicates, pred_relation, rg_first_row, rg_num_rows);
    }

private:
    ColumnReader* _reader; // non-owning; owned by ShreddedFieldNode inside VariantColumnReader
};

} // namespace starrocks::parquet
