// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/beta_rowset_reader.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "beta_rowset_reader.h"

#include <memory>

#include "storage/delete_handler.h"
#include "storage/generic_iterators.h"
#include "storage/row_block.h"
#include "storage/row_block2.h"
#include "storage/row_cursor.h"
#include "storage/rowset/segment_v2/segment_iterator.h"
#include "storage/schema.h"

namespace starrocks {

BetaRowsetReader::BetaRowsetReader(BetaRowsetSharedPtr rowset) : _rowset(std::move(rowset)), _stats(&_owned_stats) {
    _rowset->acquire();
}

OLAPStatus BetaRowsetReader::init(RowsetReaderContext* read_context) {
    auto st = _rowset->load();
    if (!st.ok()) {
        LOG(WARNING) << "failed to load rowset, st=" << st.to_string();
        return OLAP_ERR_ROWSET_LOAD_FAILED;
    }
    _context = read_context;
    if (_context->stats != nullptr) {
        // schema change/compaction should use owned_stats
        // When doing schema change/compaction,
        // only statistics of this RowsetReader is necessary.
        _stats = _context->stats;
    }
    // SegmentIterator will load seek columns on demand
    Schema schema(_context->tablet_schema->columns(), *(_context->return_columns));

    // convert RowsetReaderContext to StorageReadOptions
    StorageReadOptions read_options;
    read_options.block_mgr = read_context->block_mgr;
    read_options.stats = _stats;
    read_options.conditions = read_context->conditions;
    if (read_context->lower_bound_keys != nullptr) {
        for (int i = 0; i < read_context->lower_bound_keys->size(); ++i) {
            read_options.key_ranges.emplace_back(
                    read_context->lower_bound_keys->at(i), read_context->is_lower_keys_included->at(i),
                    read_context->upper_bound_keys->at(i), read_context->is_upper_keys_included->at(i));
        }
    }
    if (read_context->delete_handler != nullptr) {
        read_context->delete_handler->get_delete_conditions_after_version(_rowset->end_version(),
                                                                          &read_options.delete_conditions);
    }
    read_options.column_predicates = read_context->predicates;
    read_options.use_page_cache = read_context->use_page_cache;

    // create iterator for each segment
    std::vector<std::unique_ptr<RowwiseIterator>> seg_iterators;
    for (auto& seg_ptr : _rowset->_segments) {
        std::unique_ptr<RowwiseIterator> iter;
        auto s = seg_ptr->new_iterator(schema, read_options, &iter);
        if (!s.ok()) {
            LOG(WARNING) << "failed to create iterator[" << seg_ptr->id() << "]: " << s.to_string();
            return OLAP_ERR_ROWSET_READER_INIT;
        }
        seg_iterators.push_back(std::move(iter));
    }
    std::vector<RowwiseIterator*> iterators;
    iterators.reserve(seg_iterators.size());
    for (auto& owned_it : seg_iterators) {
        // transfer ownership of segment iterator to `_iterator`
        iterators.push_back(owned_it.release());
    }

    // merge or union segment iterator
    RowwiseIterator* final_iterator;
    if (read_context->need_ordered_result && _rowset->rowset_meta()->is_segments_overlapping()) {
        final_iterator = new_merge_iterator(iterators);
    } else {
        final_iterator = new_union_iterator(iterators);
    }
    auto s = final_iterator->init(read_options);
    if (!s.ok()) {
        LOG(WARNING) << "failed to init iterator: " << s.to_string();
        return OLAP_ERR_ROWSET_READER_INIT;
    }
    _iterator.reset(final_iterator);

    // init input block
    _input_block = std::make_unique<RowBlockV2>(schema, read_context->chunk_size);

    // init output block and row
    _output_block = std::make_unique<RowBlock>(read_context->tablet_schema);
    RowBlockInfo output_block_info;
    output_block_info.row_num = read_context->chunk_size;
    output_block_info.null_supported = true;
    // the output block's schema should be seek_columns to comform to v1
    // TODO(hkp): this should be optimized to use return_columns
    output_block_info.column_ids = *(_context->seek_columns);
    RETURN_NOT_OK(_output_block->init(output_block_info));
    _row = std::make_unique<RowCursor>();
    RETURN_NOT_OK(_row->init(*(read_context->tablet_schema), *(_context->seek_columns)));

    return OLAP_SUCCESS;
}

OLAPStatus BetaRowsetReader::next_block(RowBlock** block) {
    SCOPED_RAW_TIMER(&_stats->block_fetch_ns);
    // read next input block
    _input_block->clear();
    {
        auto s = _iterator->next_batch(_input_block.get());
        if (!s.ok()) {
            if (s.is_end_of_file()) {
                *block = nullptr;
                return OLAP_ERR_DATA_EOF;
            }
            LOG(WARNING) << "failed to read next block: " << s.to_string();
            return OLAP_ERR_ROWSET_READ_FAILED;
        }
    }

    // convert to output block
    _output_block->clear();
    {
        SCOPED_RAW_TIMER(&_stats->block_convert_ns);
        _input_block->convert_to_row_block(_row.get(), _output_block.get());
    }
    *block = _output_block.get();
    return OLAP_SUCCESS;
}

} // namespace starrocks
