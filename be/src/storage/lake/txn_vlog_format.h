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

#include <cstdint>
#include <cstring>
#include <memory>
#include <string_view>

#include "common/statusor.h"
#include "gen_cpp/lake_types.pb.h"
#include "storage/protobuf_file.h"

namespace starrocks::lake {

// Little-endian bytes: FE "MSVLOG1". The first byte has the invalid protobuf wire type 6, so a
// pre-group reader rejects this header instead of accepting an unknown field as an empty TxnLogPB.
// Keep it distinct from LAKE_META_HEADER_MAGIC_NUMBER: the payload is a CombinedTxnLogPB containing
// ordered statements of one tablet, not a single TxnLogPB or a combined log spanning tablets.
inline constexpr uint64_t kTxnVlogGroupMagic = 0x31474F4C56534DFEULL;

// The leading magic of a version log: kTxnVlogGroupMagic for a statement group,
// LAKE_META_HEADER_MAGIC_NUMBER for a checksummed legacy log, anything else for a headerless one,
// and 0 when |content| is shorter than a magic.
inline uint64_t txn_vlog_magic(std::string_view content) {
    uint64_t magic = 0;
    if (content.size() >= sizeof(magic)) {
        std::memcpy(&magic, content.data(), sizeof(magic));
    }
    return magic;
}

enum class TxnLogInspectionMode { SINGLE, COMBINED };

// Offline inspection must retain the persisted fields, without the publishing path's normalization
// or semantic validation. A group is self-identifying and both inspection commands print every
// statement; legacy header/plain logs retain the message type selected by the command.
inline StatusOr<std::unique_ptr<google::protobuf::Message>> decode_txn_log_for_inspection(std::string_view content,
                                                                                          TxnLogInspectionMode mode) {
    const bool grouped = txn_vlog_magic(content) == kTxnVlogGroupMagic;
    std::unique_ptr<google::protobuf::Message> message;
    if (grouped || mode == TxnLogInspectionMode::COMBINED) {
        message = std::make_unique<CombinedTxnLogPB>();
    } else {
        message = std::make_unique<TxnLogPB>();
    }
    RETURN_IF_ERROR(ProtobufFileWithHeader::load_from_buffer(
            message.get(), content, grouped ? kTxnVlogGroupMagic : LAKE_META_HEADER_MAGIC_NUMBER,
            /*allow_plain_protobuf_fallback=*/!grouped));
    return message;
}

} // namespace starrocks::lake
