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

#include "serde/encode_context.h"

#include "gen_cpp/data.pb.h" // ChunkPB

namespace starrocks::serde {

EncodeContext::EncodeContext(const int col_num, const int encode_level) : _session_encode_level(encode_level) {
    for (auto i = 0; i < col_num; ++i) {
        _column_encode_level.emplace_back(_session_encode_level);
        _raw_bytes.emplace_back(0);
        _encoded_bytes.emplace_back(0);
    }
    DCHECK(_session_encode_level != 0);
    // the lowest bit is set and other bits are not zero, then enable adjust.
    if (_session_encode_level & 1 && (_session_encode_level >> 1)) {
        _enable_adjust = true;
    }
}

void EncodeContext::update(const int col_id, uint64_t mem_bytes, uint64_t encode_byte) {
    DCHECK(_session_encode_level != 0);
    if (!_enable_adjust) {
        return;
    }
    // decide to encode or not by the encoding ratio of the first EncodeSamplingNum of every _frequency chunks
    if (_times % _frequency < EncodeSamplingNum) {
        _raw_bytes[col_id] += mem_bytes;
        _encoded_bytes[col_id] += encode_byte;
    }
}

// if encode ratio < EncodeRatioLimit, encode it, otherwise not.
void EncodeContext::_adjust(const int col_id) {
    auto old_level = _column_encode_level[col_id];
    if (_encoded_bytes[col_id] < _raw_bytes[col_id] * EncodeRatioLimit) {
        _column_encode_level[col_id] = _session_encode_level;
    } else {
        _column_encode_level[col_id] = 0;
    }
    if (old_level != _column_encode_level[col_id] || _session_encode_level < -1) {
        VLOG_ROW << "Old encode level " << old_level << " is changed to " << _column_encode_level[col_id]
                 << " because the first " << EncodeSamplingNum << " of " << _frequency << " in total " << _times
                 << " chunks' compression ratio is " << _encoded_bytes[col_id] * 1.0 / _raw_bytes[col_id]
                 << " higher than limit " << EncodeRatioLimit;
    }
    _encoded_bytes[col_id] = 0;
    _raw_bytes[col_id] = 0;
}

void EncodeContext::set_encode_levels_in_pb(ChunkPB* const res) {
    res->mutable_encode_level()->Reserve(static_cast<int>(_column_encode_level.size()));
    for (const auto& level : _column_encode_level) {
        res->mutable_encode_level()->Add(level);
    }
    adjust_encode_levels();
}

void EncodeContext::adjust_encode_levels() {
    ++_times;
    // must adjust after writing the current encode_level
    if (_enable_adjust && (_times % _frequency == EncodeSamplingNum)) {
        for (auto col_id = 0; col_id < _column_encode_level.size(); ++col_id) {
            _adjust(col_id);
        }
        _frequency = _frequency > 1000000000 ? _frequency : _frequency * 2;
    }
}
} // namespace starrocks::serde