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

#include "formats/parquet/column_reader.h"

namespace starrocks::parquet {

class ListColumnReader : public ColumnReader {
public:
    explicit ListColumnReader(const ColumnReaderOptions& opts) {}
    ~ListColumnReader() override = default;

    Status init(const ParquetField* field, std::unique_ptr<ColumnReader> element_reader) {
        _field = field;
        _element_reader = std::move(element_reader);
        return Status::OK();
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnPtr& dst) override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _element_reader->get_levels(def_levels, rep_levels, num_levels);
    }

    void set_need_parse_levels(bool need_parse_levels) override {
        _element_reader->set_need_parse_levels(need_parse_levels);
    }

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOType type, bool active) override {
        _element_reader->collect_column_io_range(ranges, end_offset, type, active);
    }

    void select_offset_index(const SparseRange<uint64_t>& range, const uint64_t rg_first_row) override {
        _element_reader->select_offset_index(range, rg_first_row);
    }

private:
    const ParquetField* _field = nullptr;
    std::unique_ptr<ColumnReader> _element_reader;
};

class MapColumnReader : public ColumnReader {
public:
    explicit MapColumnReader() = default;
    ~MapColumnReader() override = default;

    Status init(const ParquetField* field, std::unique_ptr<ColumnReader> key_reader,
                std::unique_ptr<ColumnReader> value_reader) {
        _field = field;
        _key_reader = std::move(key_reader);
        _value_reader = std::move(value_reader);

        // Check must has one valid column reader
        if (_key_reader == nullptr && _value_reader == nullptr) {
            return Status::InternalError("No avaliable parquet subfield column reader in MapColumn");
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
                                 ColumnIOType type, bool active) override {
        if (_key_reader != nullptr) {
            _key_reader->collect_column_io_range(ranges, end_offset, type, active);
        }
        if (_value_reader != nullptr) {
            _value_reader->collect_column_io_range(ranges, end_offset, type, active);
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
    const ParquetField* _field = nullptr;
    std::unique_ptr<ColumnReader> _key_reader;
    std::unique_ptr<ColumnReader> _value_reader;
};

class StructColumnReader : public ColumnReader {
public:
    explicit StructColumnReader() = default;
    ~StructColumnReader() override = default;

    Status init(const ParquetField* field, std::map<std::string, std::unique_ptr<ColumnReader>>&& child_readers) {
        _field = field;
        _child_readers = std::move(child_readers);

        if (_child_readers.empty()) {
            return Status::InternalError("No avaliable parquet subfield column reader in StructColumn");
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

    Status filter_dict_column(const ColumnPtr& column, Filter* filter, const std::vector<std::string>& sub_field_path,
                              const size_t& layer) override;

    Status fill_dst_column(ColumnPtr& dst, ColumnPtr& src) override;

    void collect_column_io_range(std::vector<io::SharedBufferedInputStream::IORange>* ranges, int64_t* end_offset,
                                 ColumnIOType type, bool active) override {
        for (const auto& pair : _child_readers) {
            if (pair.second != nullptr) {
                pair.second->collect_column_io_range(ranges, end_offset, type, active);
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

private:
    void _handle_null_rows(uint8_t* is_nulls, bool* has_null, size_t num_rows);

    // _field is generated by parquet format, so it's child order may different from _child_readers.
    const ParquetField* _field = nullptr;
    // _children_readers order is the same as TypeDescriptor children order.
    std::map<std::string, std::unique_ptr<ColumnReader>> _child_readers;
    // First non-nullptr child ColumnReader, used to get def & rep levels
    const std::unique_ptr<ColumnReader>* _def_rep_level_child_reader = nullptr;
};

} // namespace starrocks::parquet