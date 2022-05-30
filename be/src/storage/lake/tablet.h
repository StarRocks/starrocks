// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <string>

#include "common/statusor.h"
#include "storage/lake/tablet_metadata.h"

namespace starrocks::lake {

class MetadataIterator;
class TabletManager;
class TabletReader;
class TxnLog;

class Tablet {
public:
    // group: the URI of the storage group for this tablet, e.g, "s3://bucket/serviceID/groupID/"
    explicit Tablet(TabletManager* mgr, std::string group, int64_t id);
    int64_t id() const { return _id; }

    std::string group() const { return _group; }

    Status put_metadata(const TabletMetadata& metadata);

    StatusOr<TabletMetadata> get_metadata(int64_t version);

    StatusOr<MetadataIterator> list_metadata();

    Status delete_metadata(int64_t version);

    Status delete_metadata();

    Status put_txnlog(const TxnLog& log);

    StatusOr<TxnLog> get_txnlog(int64_t txn_id);

    Status delete_txnlog(int64_t txn_id);

private:
    TabletManager* _mgr;
    std::string _group;
    int64_t _id;
};

} // namespace starrocks::lake
