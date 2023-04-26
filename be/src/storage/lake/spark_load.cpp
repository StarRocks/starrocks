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

#include "storage/lake/spark_load.h"

#include "storage/lake/filenames.h"
#include "storage/lake/tablet_writer.h"
#include "storage/push_utils.h"
#include "storage/storage_engine.h"
#include "storage/tablet_manager.h"

namespace starrocks::lake {

using PushBrokerReader = starrocks::PushBrokerReader;

Status SparkLoadHandler::process_streaming_ingestion(Tablet& tablet, const TPushReq& request, PushType push_type,
                                                     std::vector<TTabletInfo>* tablet_info_vec) {
    LOG(INFO) << "begin to realtime vectorized push. tablet=" << tablet.id() << ", txn_id=" << request.transaction_id;
    DCHECK_EQ(push_type, PUSH_NORMAL_V2);

    _request = request;
    Status st = _load_convert(tablet);
    if (st.ok()) {
        if (tablet_info_vec != nullptr) {
            _get_tablet_infos(tablet, tablet_info_vec);
        }
        LOG(INFO) << "process realtime vectorized push successfully. "
                  << "tablet=" << tablet.id() << ", partition_id=" << request.partition_id
                  << ", txn_id=" << request.transaction_id;
    }

    return st;
}

Status SparkLoadHandler::_load_convert(Tablet& cur_tablet) {
    Status st;

    VLOG(3) << "start to convert delta file.";

    // 1. init writer
    ASSIGN_OR_RETURN(auto writer, cur_tablet.new_writer());
    RETURN_IF_ERROR(writer->open());
    DeferOp defer([&]() { writer->close(); });

    ASSIGN_OR_RETURN(auto tablet_schema, cur_tablet.get_schema());
    Schema schema = ChunkHelper::convert_schema(*tablet_schema);
    ChunkPtr chunk = ChunkHelper::new_chunk(schema, 0);
    auto char_field_indexes = ChunkHelper::get_char_field_indexes(schema);

    // 2. Init PushBrokerReader to read broker file if exist,
    //    in case of empty push this will be skipped.
    std::string path = _request.broker_scan_range.ranges[0].path;
    LOG(INFO) << "tablet=" << cur_tablet.id() << ", file path=" << path
              << ", file size=" << _request.broker_scan_range.ranges[0].file_size;

    if (!path.empty()) {
        auto reader = std::make_unique<PushBrokerReader>();
        DeferOp reader_close([&reader] { return reader->close(); });

        // 3. read data and write rowset
        // init Reader
        // star_offset and size are not set in FE plan before,
        // here we set if start_offset or size <= 0 for smooth upgrade.
        TBrokerScanRange t_scan_range = _request.broker_scan_range;
        DCHECK_EQ(1, t_scan_range.ranges.size());
        if (t_scan_range.ranges[0].start_offset <= 0 || t_scan_range.ranges[0].size <= 0) {
            t_scan_range.ranges[0].__set_start_offset(0);
            t_scan_range.ranges[0].__set_size(_request.broker_scan_range.ranges[0].file_size);
        }
        st = reader->init(t_scan_range, _request);
        if (!st.ok()) {
            LOG(WARNING) << "fail to init reader. res=" << st.to_string() << ", tablet=" << cur_tablet.id();
            return st;
        }

        // read data from broker and write into cur_tablet
        VLOG(3) << "start to convert etl file to delta.";
        while (!reader->eof()) {
            st = reader->next_chunk(&chunk);
            if (!st.ok()) {
                LOG(WARNING) << "Fail to read broker: " << st;
                return st;
            } else {
                if (reader->eof()) {
                    break;
                }

                ChunkHelper::padding_char_columns(char_field_indexes, schema, *tablet_schema, chunk.get());
                RETURN_IF_ERROR(writer->write(*chunk));
                chunk->reset();
            }
        }

        reader->print_profile();
        RETURN_IF_ERROR(writer->finish());
    }

    // finish
    auto txn_log = std::make_shared<TxnLog>();
    txn_log->set_tablet_id(cur_tablet.id());
    txn_log->set_txn_id(_request.transaction_id);
    auto op_write = txn_log->mutable_op_write();
    for (auto& f : writer->files()) {
        if (is_segment(f)) {
            op_write->mutable_rowset()->add_segments(std::move(f));
        } else {
            return Status::InternalError(fmt::format("unknown file {}", f));
        }
    }
    op_write->mutable_rowset()->set_num_rows(writer->num_rows());
    op_write->mutable_rowset()->set_data_size(writer->data_size());
    op_write->mutable_rowset()->set_overlapped(false);
    RETURN_IF_ERROR(cur_tablet.put_txn_log(std::move(txn_log)));

    _write_bytes += static_cast<int64_t>(writer->data_size());
    _write_rows += static_cast<int64_t>(writer->num_rows());
    VLOG(10) << "convert delta file end. res=" << st.to_string() << ", tablet=" << cur_tablet.id() << ", processed_rows"
             << writer->num_rows();
    return st;
}

void SparkLoadHandler::_get_tablet_infos(const Tablet& tablet, std::vector<TTabletInfo>* tablet_info_vec) {
    TTabletInfo tablet_info;
    tablet_info.tablet_id = tablet.id();
    tablet_info.schema_hash = 0;
    tablet_info_vec->push_back(tablet_info);
}

} // namespace starrocks::lake
