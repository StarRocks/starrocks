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

#include <memory>
#include <string>
#include <string_view>

#include "common/statusor.h"
#include "fs/fs.h"
#include "gen_cpp/types.pb.h"
#include "storage/base_tablet.h"
#include "storage/lake/metadata_iterator.h"
#include "storage/lake/tablet_metadata.h"
#include "storage/lake/txn_log.h"
#include "storage/lake/types_fwd.h"

namespace starrocks {
class TabletSchema;
class ThreadPool;
} // namespace starrocks

namespace starrocks {
class Schema;
} // namespace starrocks

namespace starrocks::lake {

class TabletManager;
class TabletReader;
class TabletWriter;
template <typename T>
class MetadataIterator;
using TabletMetadataIter = MetadataIterator<TabletMetadataPtr>;
class UpdateManager;
enum WriterType : int;

class Tablet : public BaseTablet {
public:
    explicit Tablet(TabletManager* mgr, int64_t id) : _mgr(mgr), _id(id) {
        if (_mgr != nullptr) {
            _location_provider = _mgr->location_provider();
        }
    }

    explicit Tablet(TabletManager* mgr, int64_t id, std::shared_ptr<LocationProvider> location_provider,
                    TabletMetadataPtr tablet_metadata)
            : _mgr(mgr), _id(id) {
        _location_provider = std::move(location_provider);
        _tablet_metadata = tablet_metadata;
    }

    explicit Tablet(TabletManager* mgr, int64_t id, std::shared_ptr<LocationProvider> location_provider,
                    std::shared_ptr<TabletSchema> tablet_schema)
            : _mgr(mgr), _id(id) {
        _location_provider = std::move(location_provider);
        _tablet_schema = tablet_schema;
    }

    ~Tablet() override = default;

    [[nodiscard]] int64_t id() const { return _id; }

    [[nodiscard]] int64_t tablet_id() const override { return _id; }

    [[nodiscard]] std::string root_location() const;

    Status put_metadata(const TabletMetadata& metadata);

    Status put_metadata(const TabletMetadataPtr& metadata);

    StatusOr<TabletMetadataPtr> get_metadata(int64_t version);

    Status delete_metadata(int64_t version);

    Status metadata_exists(int64_t version);

    Status put_txn_log(const TxnLog& log);

    Status put_txn_log(const TxnLogPtr& log);

    Status put_txn_slog(const TxnLogPtr& log);

    Status put_combined_txn_log(const CombinedTxnLogPB& logs);

    StatusOr<TxnLogPtr> get_txn_log(int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_slog(int64_t txn_id);

    StatusOr<TxnLogPtr> get_txn_vlog(int64_t version);

    // `segment_max_rows` is used in vertical writer
    // NOTE: This method may update the version hint
    StatusOr<std::unique_ptr<TabletWriter>> new_writer(WriterType type, int64_t txn_id,
                                                       uint32_t max_rows_per_segment = 0,
                                                       ThreadPool* flush_pool = nullptr, bool is_compaction = false);

    const std::shared_ptr<const TabletSchema> tablet_schema() const override;

    // NOTE: This method may update the version hint
    StatusOr<std::shared_ptr<const TabletSchema>> get_schema();

    StatusOr<std::shared_ptr<const TabletSchema>> get_schema_by_id(int64_t schema_id);

    StatusOr<std::vector<RowsetPtr>> get_rowsets(int64_t version);

    std::vector<RowsetPtr> get_rowsets(const TabletMetadataPtr& metadata);

    [[nodiscard]] std::string metadata_location(int64_t version) const;

    [[nodiscard]] std::string metadata_root_location() const;

    [[nodiscard]] std::string txn_log_location(int64_t txn_id) const;

    [[nodiscard]] std::string txn_slog_location(int64_t txn_id) const;

    [[nodiscard]] std::string txn_vlog_location(int64_t version) const;

    [[nodiscard]] std::string segment_location(std::string_view segment_name) const;

    [[nodiscard]] std::string del_location(std::string_view del_name) const;

    [[nodiscard]] std::string delvec_location(std::string_view delvec_name) const;

    [[nodiscard]] std::string sst_location(std::string_view sst_name) const;

    Status delete_data(int64_t txn_id, const DeletePredicatePB& delete_predicate);

    StatusOr<bool> has_delete_predicates(int64_t version);

    StatusOr<bool> has_delete_predicates(const Version& version) override {
        for (int64_t current_version = version.first; current_version < version.second; current_version++) {
            ASSIGN_OR_RETURN(auto metadata, get_metadata(current_version));
            for (const auto& rowset : metadata->rowsets()) {
                if (rowset.has_delete_predicate() && rowset.delete_predicate().version() >= version.first) {
                    return true;
                }
            }
        };
        return false;
    }

    UpdateManager* update_mgr() const { return _mgr->update_mgr(); }

    TabletManager* tablet_mgr() const { return _mgr; }

    // Many tablet operations need to fetch the tablet schema information
    // stored in the object storage, if the cache does not hit. In order to
    // reduce the costly listDirectory/listObject operations, you can specify
    // an existing tablet metadata version, so you can directly obtain the schema
    // information by reading the metadata of that version, without listObject.
    //
    // NOTE: set this value to a non-positive value means clear the version hint.
    // NOTE: Some methods of Tablet will internally update this value automatically.
    void set_version_hint(int64_t version_hint) { _version_hint = version_hint; }

    int64_t data_size();

    const std::shared_ptr<LocationProvider>& location_provider() const { return _location_provider; }

    size_t num_rows() const override;

    bool belonged_to_cloud_native() const override { return true; }

private:
    TabletManager* _mgr;
    int64_t _id;
    int64_t _version_hint = 0;
    std::shared_ptr<LocationProvider> _location_provider;
    TabletMetadataPtr _tablet_metadata;
    std::shared_ptr<TabletSchema> _tablet_schema;
};

} // namespace starrocks::lake
