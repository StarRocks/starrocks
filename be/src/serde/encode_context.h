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

#include <streamvbyte.h>

#include <string_view>
#include <vector>

#include "column/chunk.h"
#include "column/chunk_extra_data.h"
#include "common/statusor.h"

namespace starrocks {
class ChunkPB;
}
namespace starrocks::serde {
constexpr double EncodeRatioLimit = 0.9;
constexpr uint32_t EncodeSamplingNum = 5;

// EncodeContext adaptively adjusts encode_level according to the compression ratio. In detail,
// for every _frequency chunks, if the compression ratio for the first EncodeSamplingNum chunks is less than
// EncodeRatioLimit, then encode the rest chunks, otherwise not.

class EncodeContext {
public:
    static std::shared_ptr<EncodeContext> get_encode_context_shared_ptr(const int col_num, const int encode_level) {
        return encode_level == 0 ? nullptr : std::make_shared<EncodeContext>(col_num, encode_level);
    }
    EncodeContext(const int col_num, const int encode_level);
    // update encode_level for each column
    void update(const int col_id, uint64_t mem_bytes, uint64_t encode_byte);

    int get_encode_level(const int col_id) {
        DCHECK(_session_encode_level != 0);
        return _column_encode_level[col_id];
    }

    const std::vector<uint32_t>& get_encode_levels() const {
        DCHECK(_session_encode_level != 0);
        return _column_encode_level;
    }

    int get_session_encode_level() {
        DCHECK(_session_encode_level != 0);
        return _session_encode_level;
    }

    void set_encode_levels_in_pb(ChunkPB* const res);

    // adjust encode levels for each column,
    // it must be called once after each chunk is encoded
    void adjust_encode_levels();

    static constexpr uint16_t STREAMVBYTE_PADDING_SIZE = STREAMVBYTE_PADDING;

    static bool enable_encode_integer(const int encode_level) { return encode_level & ENCODE_INTEGER; }

    static bool enable_encode_string(const int encode_level) { return encode_level & ENCODE_STRING; }

private:
    static constexpr int ENCODE_INTEGER = 2;
    static constexpr int ENCODE_STRING = 4;

    // if encode ratio < EncodeRatioLimit, encode it, otherwise not.
    void _adjust(const int col_id);
    const int _session_encode_level;
    uint64_t _times = 0;
    uint64_t _frequency = 64;
    bool _enable_adjust = false;
    std::vector<uint64_t> _raw_bytes, _encoded_bytes;
    std::vector<uint32_t> _column_encode_level;
};
} // namespace starrocks::serde