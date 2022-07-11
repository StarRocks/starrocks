// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#include <memory>
#include <string>
#include <string_view>

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
} // namespace starrocks::vectorized

namespace starrocks::lake {

class TabletManager;
class TabletReader;
class TabletWriter;
template <typename T>
class MetadataIterator;
using TabletMetadataIter = MetadataIterator<TabletMetadataPtr>;

class Tablet {
public:
    explicit Tablet(TabletManager* mgr, int64_t id) : _mgr(mgr), _id(id) {}

    ~Tablet() = default;

    // Default copy and assign
    Tablet(const Tablet&) = default;
    Tablet& operator=(const Tablet&) = default;

    // Default move copy and move assign
    Tablet(Tablet&&) = default;
    Tablet& operator=(Tablet&&) = default;

    [[nodiscard]] int64_t id() const { return _id; }

    [[nodiscard]] std::string root_location() const;

    Status put_metadata(const TabletMetadata& metadata);

    Status put_metadata(TabletMetadataPtr metadata);

    StatusOr<TabletMetadataPtr> get_metadata(int64_t version);

    Status delete_metadata(int64_t version);

    Status put_txn_log(const TxnLog& log);

    Status put_txn_log(TxnLogPtr log);

    StatusOr<TxnLogPtr> get_txn_log(int64_t txn_id);

    Status delete_txn_log(int64_t txn_id);

    StatusOr<std::unique_ptr<TabletWriter>> new_writer();

    StatusOr<std::shared_ptr<TabletReader>> new_reader(int64_t version, vectorized::Schema schema);

    StatusOr<std::shared_ptr<const TabletSchema>> get_schema();

    StatusOr<std::vector<RowsetPtr>> get_rowsets(int64_t version);

    [[nodiscard]] std::string metadata_location(int64_t version) const;

    [[nodiscard]] std::string txn_log_location(int64_t txn_id) const;

    [[nodiscard]] std::string segment_location(std::string_view segment_name) const;

private:
    TabletManager* _mgr;
    int64_t _id;
};

} // namespace starrocks::lake
