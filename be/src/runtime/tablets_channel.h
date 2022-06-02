// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <sstream>
#include <string>

#include "gen_cpp/Types_types.h"
#include "gutil/ref_counted.h"

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

class TabletsChannel : public RefCountedThreadSafe<TabletsChannel> {
public:
    TabletsChannel() = default;

    virtual Status open(const PTabletWriterOpenRequest& params) = 0;

    virtual void add_chunk(brpc::Controller* cntl, const PTabletWriterAddChunkRequest& request,
                           PTabletWriterAddBatchResult* response, google::protobuf::Closure* done) = 0;

    virtual void cancel() = 0;

private:
    friend class RefCountedThreadSafe<TabletsChannel>;
    friend class LocalTabletsChannel;

    virtual ~TabletsChannel() = default;
};

struct TabletsChannelKey {
    UniqueId id;
    int64_t index_id;

    TabletsChannelKey(const PUniqueId& pid, int64_t index_id_) : id(pid), index_id(index_id_) {}

    ~TabletsChannelKey() noexcept = default;

    bool operator==(const TabletsChannelKey& rhs) const noexcept { return index_id == rhs.index_id && id == rhs.id; }

    std::string to_string() const;
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
