// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/rowset/beta_rowset.h

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

#ifndef STARROCKS_SRC_OLAP_ROWSET_BETA_ROWSET_H_
#define STARROCKS_SRC_OLAP_ROWSET_BETA_ROWSET_H_

#include "common/statusor.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_reader.h"
#include "storage/rowset/segment_v2/segment.h"

namespace starrocks {

class BetaRowsetReader;
class RowsetFactory;

class BetaRowset;
using BetaRowsetSharedPtr = std::shared_ptr<BetaRowset>;
class DelVector;
using DelVectorPtr = std::shared_ptr<DelVector>;
class KVStore;

class BetaRowset : public Rowset {
public:
    BetaRowset(MemTracker* mem_tracker, const TabletSchema* schema, std::string rowset_path,
               RowsetMetaSharedPtr rowset_meta);
    ~BetaRowset() override { _mem_tracker->release(_mem_tracker->consumption()); }

    OLAPStatus create_reader(RowsetReaderSharedPtr* result) override;

    StatusOr<vectorized::ChunkIteratorPtr> new_iterator(const vectorized::Schema& schema,
                                                        const vectorized::RowsetReadOptions& options) override;

    Status get_segment_iterators(const vectorized::Schema& schema, const vectorized::RowsetReadOptions& options,
                                 std::vector<vectorized::ChunkIteratorPtr>* seg_iterators) override;

    // only used for updatable tablets' rowset
    // simply get iterators to iterate all rows without complex options like predicates
    // |schema| read schema
    // |meta| olap meta, used for get delvec, if null do not fetch&use delvec
    // |version| read version, use for get delvec
    // |stats| used for iterator read stats
    // return iterator list, an iterator for each segment,
    // if the segment is empty, put an empty pointer in list
    // caller is also responsible to call rowset's acquire/release
    StatusOr<std::vector<vectorized::ChunkIteratorPtr>> get_segment_iterators2(const vectorized::Schema& schema,
                                                                               KVStore* meta, int64_t version,
                                                                               OlapReaderStatistics* stats);

    static std::string segment_file_path(const std::string& segment_dir, const RowsetId& rowset_id, int segment_id);

    static std::string segment_temp_file_path(const std::string& dir, const RowsetId& rowset_id, int segment_id);

    static std::string segment_del_file_path(const std::string& segment_dir, const RowsetId& rowset_id, int segment_id);

    static std::string segment_srcrssid_file_path(const std::string& segment_dir, const RowsetId& rowset_id,
                                                  int segment_id);

    OLAPStatus split_range(const RowCursor& start_key, const RowCursor& end_key, uint64_t request_block_row_count,
                           std::vector<OlapTuple>* ranges) override;

    OLAPStatus remove() override;

    Status link_files_to(const std::string& dir, RowsetId new_rowset_id) override;

    OLAPStatus copy_files_to(const std::string& dir) override;

    bool check_path(const std::string& path) override;

    std::vector<segment_v2::SegmentSharedPtr>& segments() { return _segments; }

protected:
    // init segment groups
    OLAPStatus init() override;

    Status do_load() override;

    void do_close() override;

private:
    friend class RowsetFactory;
    friend class BetaRowsetReader;
    std::vector<segment_v2::SegmentSharedPtr> _segments;
};

} // namespace starrocks

#endif //STARROCKS_SRC_OLAP_ROWSET_BETA_ROWSET_H_
