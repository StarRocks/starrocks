// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <string>

#include "common/statusor.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/rowset.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"

namespace starrocks {
class TabletSchema;
}

namespace starrocks::vectorized {
class Schema;
}

namespace starrocks::lake {

class TabletManager;
class TabletReader;
class TabletWriter;
template <typename T>
class MetadataIterator;
using TabletMetadataIter = MetadataIterator<TabletMetadataPtr>;

class Tablet {
public:
    // group: the URI of the storage group for this tablet, e.g, "s3://bucket/serviceID/groupID/"
    explicit Tablet(TabletManager* mgr, std::string group, int64_t id) : _mgr(mgr), _root(std::move(group)), _id(id) {}

    ~Tablet() = default;

    // Default copy and assign
    Tablet(const Tablet&) = default;
    Tablet& operator=(const Tablet&) = default;

    // Default move copy and move assign
    Tablet(Tablet&&) = default;
    Tablet& operator=(Tablet&&) = default;

    int64_t id() const { return _id; }

    std::string root() const { return _root; }

    std::string root_location() const;

    Status put_metadata(const TabletMetadata& metadata);

    Status put_metadata(TabletMetadataPtr metadata);

    StatusOr<TabletMetadataPtr> get_metadata(int64_t version);

    StatusOr<TabletMetadataIter> list_metadata();

    Status delete_metadata(int64_t version);

    Status delete_metadata();

    Status put_txn_log(const TxnLog& log);

    Status put_txn_log(TxnLogPtr log);

    StatusOr<TxnLogPtr> get_txn_log(int64_t txn_id);

    Status delete_txn_log(int64_t txn_id);

    StatusOr<std::unique_ptr<TabletWriter>> new_writer();

    StatusOr<std::shared_ptr<TabletReader>> new_reader(int64_t version, vectorized::Schema schema);

    StatusOr<std::shared_ptr<const TabletSchema>> get_schema();

    StatusOr<std::vector<RowsetPtr>> get_rowsets(int64_t version);

    std::string metadata_path(int64_t version) const;

    std::string txn_log_path(int64_t txn_id) const;

    std::string segment_path_assemble(const std::string& segment_name) const;

private:
    TabletManager* _mgr;
    // TODO: remove this variable.
    std::string _root;
    int64_t _id;
};

} // namespace starrocks::lake
