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

#include "formats/parquet/stored_column_reader_with_index.h"

#include "util/defer_op.h"

namespace starrocks::parquet {

Status StoredColumnReaderWithIndex::read_range(const Range<uint64_t>& range, const Filter* filter,
                                               ColumnContentType content_type, Column* dst) {
    DCHECK(range.begin() >= _offset_index_ctx->offset_index.page_locations[_cur_page_idx].first_row_index +
                                    _offset_index_ctx->rg_first_row);
    size_t stop_page_idx = _cur_page_idx;
    while (stop_page_idx < _page_num - 1 &&
           range.end() >= _offset_index_ctx->offset_index.page_locations[stop_page_idx + 1].first_row_index +
                                  _offset_index_ctx->rg_first_row) {
        stop_page_idx++;
    }

    // for next read
    DeferOp op([&]() { _cur_page_idx = stop_page_idx; });

    // deal with dict page
    // reader move stream cursor, move reader row cursor, load specific_page
    if (!_try_dict_loaded) {
        RETURN_IF_ERROR(_inner_reader->try_load_dictionary());
        _try_dict_loaded = true;
    }

    if (_cur_page_idx == _page_num - 1 ||
        range.begin() < _offset_index_ctx->offset_index.page_locations[_cur_page_idx + 1].first_row_index +
                                _offset_index_ctx->rg_first_row) {
        return _inner_reader->read_range(range, filter, content_type, dst);
    } else {
        while (_cur_page_idx < _page_num - 1 &&
               range.begin() >= _offset_index_ctx->offset_index.page_locations[_cur_page_idx + 1].first_row_index +
                                        _offset_index_ctx->rg_first_row) {
            _cur_page_idx++;
        }
        DCHECK(_offset_index_ctx->page_selected[_cur_page_idx]);

        RETURN_IF_ERROR(_inner_reader->load_specific_page(
                _cur_page_idx, _offset_index_ctx->offset_index.page_locations[_cur_page_idx].offset,
                _offset_index_ctx->offset_index.page_locations[_cur_page_idx].first_row_index +
                        _offset_index_ctx->rg_first_row));
        return _inner_reader->read_range(range, filter, content_type, dst);
    }
}

} // namespace starrocks::parquet
