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

#include "formats/parquet/page_index_reader.h"
#include "formats/parquet/stored_column_reader.h"

namespace starrocks::parquet {

class StoredColumnReaderWithIndex : public StoredColumnReader {
public:
    StoredColumnReaderWithIndex(std::unique_ptr<StoredColumnReader> reader, ColumnOffsetIndexCtx* offset_index_ctx,
                                bool has_dict_page)
            : _inner_reader(std::move(reader)), _offset_index_ctx(offset_index_ctx), _has_dict_page(has_dict_page) {
        _page_num = _offset_index_ctx->page_selected.size();
        _inner_reader->set_page_num(_page_num);
    }

    ~StoredColumnReaderWithIndex() = default;

    void set_need_parse_levels(bool need_parse_levels) override {
        _inner_reader->set_need_parse_levels(need_parse_levels);
    }

    Status read_records(size_t* num_rows, ColumnContentType content_type, Column* dst) override {
        return _inner_reader->read_records(num_rows, content_type, dst);
    }

    Status read_range(const Range<uint64_t>& range, const Filter* filter, ColumnContentType content_type,
                      Column* dst) override;

    void get_levels(level_t** def_levels, level_t** rep_levels, size_t* num_levels) override {
        _inner_reader->get_levels(def_levels, rep_levels, num_levels);
    }

    Status get_dict_values(Column* column) override { return _inner_reader->get_dict_values(column); }

    Status get_dict_values(const std::vector<int32_t>& dict_codes, const NullableColumn& nulls,
                           Column* column) override {
        return _inner_reader->get_dict_values(dict_codes, nulls, column);
    }

private:
    std::unique_ptr<StoredColumnReader> _inner_reader;
    ColumnOffsetIndexCtx* _offset_index_ctx;
    size_t _cur_page_idx = 0;
    size_t _page_num = 0;
    bool _dict_page_loaded = false;
    bool _has_dict_page;
};

} // namespace starrocks::parquet
