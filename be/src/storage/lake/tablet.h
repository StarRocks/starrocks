// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <string>

#include "common/statusor.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks {
class TabletSchema;
}

namespace starrocks::lake {

class MetadataIterator;
class TabletManager;
class TabletReader;
class TabletWriter;

class Tablet {
public:
    // group: the URI of the storage group for this tablet, e.g, "s3://bucket/serviceID/groupID/"
    explicit Tablet(TabletManager* mgr, std::string group, int64_t id);

    ~Tablet() = default;

    // Default copy and assign
    Tablet(const Tablet&) = default;
    Tablet& operator=(const Tablet&) = default;

    // Default move copy and move assign
    Tablet(Tablet&&) = default;
    Tablet& operator=(Tablet&&) = default;

    int64_t id() const { return _id; }

    std::string group() const { return _group; }

    Status put_metadata(const TabletMetadata& metadata);

    Status put_metadata(TabletMetadataPtr metadata);

    StatusOr<TabletMetadataPtr> get_metadata(int64_t version);

    StatusOr<MetadataIterator> list_metadata();

    Status delete_metadata(int64_t version);

    Status delete_metadata();

    Status put_txn_log(const TxnLog& log);

    Status put_txn_log(TxnLogPtr log);

    StatusOr<TxnLogPtr> get_txn_log(int64_t txn_id);

    Status delete_txn_log(int64_t txn_id);

    StatusOr<std::unique_ptr<TabletWriter>> new_writer();

    StatusOr<std::shared_ptr<const TabletSchema>> get_schema();

private:
    TabletManager* _mgr;
    std::string _group;
    int64_t _id;
};

} // namespace starrocks::lake
