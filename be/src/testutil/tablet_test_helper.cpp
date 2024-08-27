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

#include "testutil/tablet_test_helper.h"

#include "storage/chunk_helper.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/tablet.h"

namespace starrocks {
TCreateTabletReq TabletTestHelper::gen_create_tablet_req(TTabletId tablet_id, TKeysType::type type,
                                                         TStorageType::type storage_type) {
    TCreateTabletReq req;
    req.tablet_id = tablet_id;
    req.__set_version(1);
    req.__set_version_hash(0);
    req.tablet_schema.schema_hash = 0;
    req.tablet_schema.short_key_column_count = 2;
    req.tablet_schema.keys_type = type;
    req.tablet_schema.storage_type = storage_type;
    return req;
}

std::shared_ptr<RowsetWriter> TabletTestHelper::create_rowset_writer(const Tablet& tablet, RowsetId rowset_id,
                                                                     Version version) {
    RowsetWriterContext writer_context;
    writer_context.rowset_id = rowset_id;
    writer_context.tablet_uid = tablet.tablet_uid();
    writer_context.tablet_id = tablet.tablet_id();
    writer_context.tablet_schema_hash = tablet.schema_hash();
    writer_context.rowset_path_prefix = tablet.schema_hash_path();
    writer_context.tablet_schema = tablet.tablet_schema();
    writer_context.rowset_state = VISIBLE;
    writer_context.version = version;
    std::unique_ptr<RowsetWriter> rowset_writer;
    auto st = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
    CHECK(st.ok());
    return rowset_writer;
}

std::shared_ptr<TabletReader> TabletTestHelper::create_rowset_reader(const TabletSharedPtr& tablet,
                                                                     const Schema& schema, Version version) {
    TabletReaderParams read_params;
    read_params.reader_type = ReaderType::READER_ALTER_TABLE;
    read_params.skip_aggregation = false;
    read_params.chunk_size = config::vector_chunk_size;
    auto tablet_reader = std::make_unique<TabletReader>(tablet, version, schema);

    CHECK(tablet_reader != nullptr);
    CHECK(tablet_reader->prepare().ok());
    CHECK(tablet_reader->open(read_params).ok());

    return tablet_reader;
}

std::shared_ptr<DeltaWriter> TabletTestHelper::create_delta_writer(TTabletId tablet_id,
                                                                   const std::vector<SlotDescriptor*>& slots,
                                                                   MemTracker* mem_tracker) {
    DeltaWriterOptions options;
    options.tablet_id = tablet_id;
    options.slots = &slots;
    options.txn_id = 1;
    options.partition_id = 1;
    options.replica_state = ReplicaState::Primary;
    auto writer = DeltaWriter::open(options, mem_tracker);
    CHECK(writer.ok());
    return std::move(writer.value());
}

std::vector<ChunkIteratorPtr> TabletTestHelper::create_segment_iterators(const Tablet& tablet, Version version,
                                                                         OlapReaderStatistics* _stats) {
    auto new_rowset = tablet.get_rowset_by_version(version);
    CHECK(new_rowset != nullptr);

    std::vector<ChunkIteratorPtr> seg_iters;
    RowsetReadOptions rowset_opts;
    rowset_opts.version = version.second;
    rowset_opts.stats = _stats;
    auto st = new_rowset->get_segment_iterators(*tablet.tablet_schema()->schema(), rowset_opts, &seg_iters);
    CHECK(st.ok());
    return seg_iters;
}
} // namespace starrocks