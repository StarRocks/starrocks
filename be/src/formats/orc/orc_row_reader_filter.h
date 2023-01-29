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

#include <orc/OrcFile.hh>

#include "exec/exec_node.h"
#include "exec/hdfs_scanner.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_min_max_decoder.h"
#include "gen_cpp/orc_proto.pb.h"
#include "storage/chunk_helper.h"

namespace starrocks {
class OrcRowReaderFilter : public orc::RowReaderFilter {
public:
    OrcRowReaderFilter(const HdfsScannerParams& scanner_params, const HdfsScannerContext& scanner_ctx,
                       OrcChunkReader* reader);
    bool filterOnOpeningStripe(uint64_t stripeIndex, const orc::proto::StripeInformation* stripeInformation) override;
    bool filterOnPickRowGroup(size_t rowGroupIdx, const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                              const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilters) override;
    bool filterMinMax(size_t rowGroupIdx, const std::unordered_map<uint64_t, orc::proto::RowIndex>& rowIndexes,
                      const std::map<uint32_t, orc::BloomFilterIndex>& bloomFilter);
    bool filterOnPickStringDictionary(const std::unordered_map<uint64_t, orc::StringDictionary*>& sdicts) override;

    bool is_slot_evaluated(SlotId id) { return _dict_filter_eval_cache.find(id) != _dict_filter_eval_cache.end(); }
    void onStartingPickRowGroups() override;
    void onEndingPickRowGroups() override;
    void setWriterTimezone(const std::string& tz) override;

private:
    const HdfsScannerParams& _scanner_params;
    const HdfsScannerContext& _scanner_ctx;
    uint64_t _current_stripe_index{0};
    bool _init_use_dict_filter_slots{false};
    std::vector<pair<SlotDescriptor*, uint64_t>> _use_dict_filter_slots;
    friend class HdfsOrcScanner;
    std::unordered_map<SlotId, FilterPtr> _dict_filter_eval_cache;
    bool _can_do_filter_on_orc_cvb{true}; // cvb: column vector batch.
    // key: end of range.
    // value: start of range.
    // ranges are not overlapped.
    // check `offset` in a range or not:
    // 1. use `upper_bound` to find the first range.end > `offset`
    // 2. and check if range.start <= `offset`
    std::map<uint64_t, uint64_t> _scan_ranges;
    OrcChunkReader* _reader;
    int64_t _writer_tzoffset_in_seconds;
};
} // namespace starrocks