// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "rowset_update_state.h"

#include "storage/primary_key_encoder.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/vectorized/chunk_helper.h"

namespace starrocks {

using vectorized::ChunkHelper;

RowsetUpdateState::RowsetUpdateState() = default;

RowsetUpdateState::~RowsetUpdateState() = default;

Status RowsetUpdateState::load(int64_t tablet_id, Rowset* rowset) {
    if (UNLIKELY(!_status.ok())) {
        return _status;
    }
    std::call_once(_load_once_flag, [&] {
        _tablet_id = tablet_id;
        _status = _do_load(rowset);
    });
    return _status;
}

Status RowsetUpdateState::_do_load(Rowset* rowset) {
    auto& schema = rowset->schema();
    vector<uint32_t> pk_columns;
    for (size_t i = 0; i < schema.num_key_columns(); i++) {
        pk_columns.push_back((uint32_t)i);
    }
    vectorized::Schema pkey_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(schema, pk_columns);
    std::unique_ptr<vectorized::Column> pk_column;
    if (!PrimaryKeyEncoder::create_column(pkey_schema, &pk_column).ok()) {
        CHECK(false) << "create column for primary key encoder failed";
    }

    auto block_manager = fs::fs_util::block_manager();
    // always one file for now.
    for (auto i = 0; i < rowset->num_delete_files(); i++) {
        auto path = BetaRowset::segment_del_file_path(rowset->rowset_path(), rowset->rowset_id(),
                                                      rowset->num_segments() - 1);
        std::unique_ptr<fs::ReadableBlock> rblock;
        RETURN_IF_ERROR(block_manager->open_block(path, &rblock));
        uint64_t file_size = 0;
        rblock->size(&file_size);
        std::string read_buffer(file_size, 0);
        Slice read_slice(read_buffer);
        rblock->read(0, read_slice);
        auto col = pk_column->clone();
        col->deserialize_column((uint8_t*)(read_buffer.data()));
        _deletes.emplace_back(std::move(col));
    }

    RowsetReleaseGuard guard(rowset->shared_from_this());
    OlapReaderStatistics stats;
    auto beta_rowset = down_cast<BetaRowset*>(rowset);
    auto res = beta_rowset->get_segment_iterators2(pkey_schema, nullptr, 0, &stats);
    if (!res.ok()) {
        return res.status();
    }
    // TODO(cbl): auto close iterators on failure
    auto& itrs = res.value();
    CHECK(itrs.size() == rowset->num_segments()) << "itrs.size != num_segments";
    _upserts.resize(rowset->num_segments());
    // only hold pkey, so can use larger chunk size
    auto chunk_shared_ptr = ChunkHelper::new_chunk(pkey_schema, 4096);
    auto chunk = chunk_shared_ptr.get();
    for (size_t i = 0; i < itrs.size(); i++) {
        auto itr = itrs[i].get();
        if (itr == nullptr) {
            continue;
        }
        auto& dest = _upserts[i];
        auto col = pk_column->clone();
        auto num_rows = beta_rowset->segments()[i]->num_rows();
        col->reserve(num_rows);
        while (true) {
            chunk->reset();
            auto st = itr->get_next(chunk);
            if (st.is_end_of_file()) {
                break;
            } else if (!st.ok()) {
                return st;
            } else {
                PrimaryKeyEncoder::encode(pkey_schema, *chunk, 0, chunk->num_rows(), col.get());
            }
        }
        itr->close();
        CHECK(col->size() == num_rows) << "read segment: iter rows != num rows";
        dest = std::move(col);
    }
    for (const auto& upsert : upserts()) {
        _memory_usage += upsert != nullptr ? upsert->memory_usage() : 0;
    }
    for (const auto& one_delete : deletes()) {
        _memory_usage += one_delete != nullptr ? one_delete->memory_usage() : 0;
    }
    return Status::OK();
}

std::string RowsetUpdateState::to_string() const {
    return Substitute("RowsetUpdateState tablet:$0", _tablet_id);
}

} // namespace starrocks
