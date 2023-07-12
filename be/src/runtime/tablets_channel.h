// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#pragma once

#include <sstream>
#include <string>

#include "column/chunk.h"
#include "common/status.h"
#include "exec/tablet_info.h"
#include "gen_cpp/Types_types.h"
#include "gen_cpp/types.pb.h"
#include "gutil/ref_counted.h"
#include "util/uid_util.h"

namespace brpc {
class Controller;
}

namespace google::protobuf {
class Closure;
};

namespace starrocks {

class LoadChannel;
class OlapTableSchemaParam;
class PTabletWriterOpenRequest;
class PTabletWriterAddBatchResult;
class PTabletWriterAddChunkRequest;
class PTabletWriterAddSegmentRequest;
class PTabletWriterAddSegmentResult;

class TabletsChannel {
public:
    TabletsChannel() = default;
    virtual ~TabletsChannel() = default;

    [[nodiscard]] virtual Status open(const PTabletWriterOpenRequest& params,
                                      std::shared_ptr<OlapTableSchemaParam> schema) = 0;

    virtual void add_chunk(vectorized::Chunk* chunk, const PTabletWriterAddChunkRequest& request,
                           PTabletWriterAddBatchResult* response) = 0;

    virtual void cancel() = 0;

    virtual void abort() = 0;

    virtual void abort(const std::vector<int64_t>& tablet_ids, const std::string& reason) = 0;
};

struct TabletsChannelKey {
    UniqueId id;
    int64_t index_id;

    TabletsChannelKey(const PUniqueId& pid, int64_t index_id_) : id(pid), index_id(index_id_) {}

    ~TabletsChannelKey() noexcept = default;

    bool operator==(const TabletsChannelKey& rhs) const noexcept { return index_id == rhs.index_id && id == rhs.id; }

    [[nodiscard]] std::string to_string() const;
};

inline std::ostream& operator<<(std::ostream& os, const TabletsChannelKey& key) {
    os << "(id=" << key.id << ",index_id=" << key.index_id << ")";
    return os;
}

inline std::string TabletsChannelKey::to_string() const {
    std::stringstream ss;
    ss << *this;
    return ss.str();
}

} // namespace starrocks
