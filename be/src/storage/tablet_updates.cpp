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

#include "storage/tablet_updates.h"

#include <cmath>
#include <ctime>
#include <memory>

#include "common/status.h"
#include "common/tracer.h"
#include "exec/schema_scanner/schema_be_tablets_scanner.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/stl_util.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "rocksdb/write_batch.h"
#include "rowset_merger.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_iterator.h"
#include "storage/compaction_utils.h"
#include "storage/del_vector.h"
#include "storage/empty_iterator.h"
#include "storage/merge_iterator.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/rowset/rowset_options.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/segment_options.h"
#include "storage/rowset_column_update_state.h"
#include "storage/rowset_update_state.h"
#include "storage/schema_change.h"
#include "storage/snapshot_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/types.h"
#include "storage/union_iterator.h"
#include "storage/update_compaction_state.h"
#include "storage/update_manager.h"
#include "util/defer_op.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"
#include "util/starrocks_metrics.h"

namespace starrocks {

std::string EditVersion::to_string() const {
    if (minor() == 0) {
        return strings::Substitute("$0", major());
    } else {
        return strings::Substitute("$0.$1", major(), minor());
    }
}

using IteratorList = TabletUpdates::IteratorList;

TabletUpdates::TabletUpdates(Tablet& tablet) : _tablet(tablet), _unused_rowsets(UINT64_MAX) {}

TabletUpdates::~TabletUpdates() {
    _stop_and_wait_apply_done();
}

template <class Itr1, class Itr2>
vector<uint32_t> modify(const vector<uint32_t>& orig, Itr1 add_begin, Itr1 add_end, Itr2 del_begin, Itr2 del_end) {
    vector<uint32_t> ret;
    ret.reserve(orig.size() + (add_end - add_begin) + (del_end - del_begin));
    for (auto v : orig) {
        // TODO: optimize when #dels is large
        if (std::find(del_begin, del_end, v) == del_end) {
            ret.push_back(v);
        }
    }
    ret.insert(ret.end(), add_begin, add_end);
    return ret;
}

template <class T, class Itr>
void repeated_field_add(::google::protobuf::RepeatedField<T>* array, Itr begin, Itr end) {
    array->Reserve(array->size() + end - begin);
    for (auto i = begin; i < end; i++) {
        array->AddAlreadyReserved(*i);
    }
}

Status TabletUpdates::init() {
    std::unique_ptr<TabletUpdatesPB> updates(_tablet.tablet_meta()->release_updates(this));
    if (!updates) {
        string msg = strings::Substitute("updatable tablet do not have updates meta tablet:$0", _tablet.tablet_id());
        _set_error(msg);
        LOG(ERROR) << msg;
        return Status::InternalError(msg);
    }
    return _load_from_pb(*updates);
}

Status TabletUpdates::_load_meta_and_log(const TabletUpdatesPB& tablet_updates_pb) {
    const auto& edit_version_meta_pbs = tablet_updates_pb.versions();
    if (edit_version_meta_pbs.empty()) {
        string msg =
                strings::Substitute("tablet_updates_pb.edit_version_meta_pbs should have at least 1 version tablet:$0",
                                    _tablet.tablet_id());
        return Status::InternalError(msg);
    }
    _edit_version_infos.clear();
    for (auto& edit_version_meta_pb : edit_version_meta_pbs) {
        _redo_edit_version_log(edit_version_meta_pb);
    }
    EditVersion apply_version(tablet_updates_pb.apply_version().major(), tablet_updates_pb.apply_version().minor());
    _sync_apply_version_idx(apply_version);

    _next_rowset_id = tablet_updates_pb.next_rowset_id();
    _next_log_id = tablet_updates_pb.next_log_id();
    auto apply_log_func = [&](uint64_t logid, const TabletMetaLogPB& tablet_meta_log_pb) -> bool {
        CHECK(!tablet_meta_log_pb.ops().empty());
        for (auto& tablet_meta_op_pb : tablet_meta_log_pb.ops()) {
            switch (tablet_meta_op_pb.type()) {
            case OP_ROWSET_COMMIT:
            case OP_COMPACTION_COMMIT:
                _redo_edit_version_log(tablet_meta_op_pb.commit());
                break;
            case OP_APPLY:
                _sync_apply_version_idx(
                        EditVersion(tablet_meta_op_pb.apply().major(), tablet_meta_op_pb.apply().minor()));
                break;
            default:
                LOG(FATAL) << "unsupported TabletMetaLogPB type: " << TabletMetaOpType_Name(tablet_meta_op_pb.type());
            }
        }
        _next_log_id = logid + 1;
        return true;
    };
    auto st = TabletMetaManager::traverse_meta_logs(_tablet.data_dir(), _tablet.tablet_id(), apply_log_func);
    if (!st.ok()) {
        return st;
    }
    DCHECK_LE(tablet_updates_pb.next_log_id(), _next_log_id) << " tabletid:" << _tablet.tablet_id();
    return st;
}

Status TabletUpdates::_load_rowsets_and_check_consistency(std::set<uint32_t>& unapplied_rowsets) {
    std::set<uint32_t> all_rowsets;
    // Load all rowsets of this tablet into memory.
    // NOTE: This may change in a near future, e.g, manage rowsets in a separate module and load
    // them on demand.
    _rowsets.clear();
    RETURN_IF_ERROR(TabletMetaManager::rowset_iterate(
            _tablet.data_dir(), _tablet.tablet_id(), [&](const RowsetMetaSharedPtr& rowset_meta) -> bool {
                RowsetSharedPtr rowset;
                auto st = RowsetFactory::create_rowset(&_tablet.tablet_schema(), _tablet.schema_hash_path(),
                                                       rowset_meta, &rowset);
                if (st.ok()) {
                    _rowsets[rowset_meta->get_rowset_seg_id()] = std::move(rowset);
                } else {
                    LOG(WARNING) << "Fail to create rowset from rowset meta. rowset=" << rowset_meta->rowset_id()
                                 << " state=" << rowset_meta->rowset_state() << " tablet:" << _tablet.tablet_id();
                }
                all_rowsets.insert(rowset_meta->get_rowset_seg_id());
                return true;
            }));

    unapplied_rowsets.clear();
    std::set<uint32_t> active_rowsets;
    std::vector<uint32_t> missing_rowsets;
    for (size_t i = 0; i < _edit_version_infos.size(); i++) {
        auto& rs = _edit_version_infos[i]->rowsets;
        for (auto rid : rs) {
            bool inserted = active_rowsets.insert(rid).second;
            if (inserted) {
                if (_rowsets.find(rid) == _rowsets.end()) {
                    missing_rowsets.push_back(rid);
                }
                if (i > _apply_version_idx) {
                    // it's a newly added rowset which have not been applied yet
                    unapplied_rowsets.insert(rid);
                }
            }
        }
    }
    if (!missing_rowsets.empty()) {
        std::string msg = strings::Substitute("tablet init missing rowset, $0 all:$1 active:$2 missing:$3",
                                              _debug_version_info(false), JoinInts(all_rowsets, ","),
                                              JoinInts(active_rowsets, ","), JoinInts(missing_rowsets, ","));
        DCHECK(false) << msg; // exit on curruption in debug mode, try to fix in release mode
        return Status::Corruption(msg);
    }

    // Find unused rowsets.
    std::vector<uint32_t> unused_rowsets;
    std::set_difference(all_rowsets.begin(), all_rowsets.end(), active_rowsets.begin(), active_rowsets.end(),
                        std::back_inserter(unused_rowsets));
    for (uint32_t id : unused_rowsets) {
        auto iter = _rowsets.find(id);
        DCHECK(iter != _rowsets.end());
        _unused_rowsets.blocking_put(std::move(iter->second));
        _rowsets.erase(iter);
        all_rowsets.erase(id);
    }
    return Status::OK();
}

Status TabletUpdates::_purge_versions_to_fix_rowset_missing_inconsistency() {
    size_t num_version_removed = _apply_version_idx;
    if (num_version_removed == 0) {
        return Status::InternalError("no version to purge when _purge_versions_to_fix_rowset_missing_inconsistency");
    }
    _edit_version_infos.erase(_edit_version_infos.begin(), _edit_version_infos.begin() + num_version_removed);
    _apply_version_idx -= num_version_removed;
    return Status::OK();
}

Status TabletUpdates::_load_pending_rowsets() {
    // Load pending rowsets
    _pending_commits.clear();
    return TabletMetaManager::pending_rowset_iterate(
            _tablet.data_dir(), _tablet.tablet_id(), [&](int64_t version, std::string_view rowset_meta_data) -> bool {
                bool parse_ok = false;
                auto rowset_meta = std::make_shared<RowsetMeta>(rowset_meta_data, &parse_ok);
                CHECK(parse_ok) << "Corrupted rowset meta";
                RowsetSharedPtr rowset;
                auto st = RowsetFactory::create_rowset(&_tablet.tablet_schema(), _tablet.schema_hash_path(),
                                                       rowset_meta, &rowset);
                if (st.ok()) {
                    _pending_commits.emplace(version, rowset);
                } else {
                    LOG(WARNING) << "Fail to create rowset from pending rowset meta. rowset="
                                 << rowset_meta->rowset_id() << " state=" << rowset_meta->rowset_state();
                }
                return true;
            });
}

Status TabletUpdates::_load_from_pb(const TabletUpdatesPB& tablet_updates_pb) {
    std::unique_lock l1(_lock);
    std::unique_lock l2(_rowsets_lock);

    RETURN_IF_ERROR(_load_meta_and_log(tablet_updates_pb));

    std::set<uint32_t> unapplied_rowsets;
    auto st = _load_rowsets_and_check_consistency(unapplied_rowsets);
    if (st.is_corruption()) {
        // keep the latest version and purge all previous versions, then try to load rowsets again.
        auto st_purge = _purge_versions_to_fix_rowset_missing_inconsistency();
        if (!st_purge.ok()) {
            st = st.clone_and_append(st_purge.get_error_msg());
            LOG(ERROR) << st;
            return st;
        }
        auto reload_st = _load_rowsets_and_check_consistency(unapplied_rowsets);
        if (!reload_st.ok()) {
            st = st.clone_and_append(" after purge:" + _debug_version_info(false) +
                                     " reload failed: " + reload_st.get_error_msg());
            LOG(ERROR) << st;
            return st;
        } else {
            LOG(WARNING) << st.get_error_msg() << " after purge:" << _debug_version_info(false) << " reload success";
        }
    } else if (!st.ok()) {
        return st;
    }

    // Load delete vectors and update RowsetStats.
    // TODO: save num_dels in rowset meta.
    std::unordered_map<uint32_t, ssize_t> del_vector_cardinality_by_rssid;
    for (auto& [rsid, rowset] : _rowsets) {
        if (unapplied_rowsets.find(rsid) == unapplied_rowsets.end()) {
            for (uint32_t i = 0; i < rowset->num_segments(); i++) {
                del_vector_cardinality_by_rssid[rsid + i] = -1;
            }
        }
    }

    RETURN_IF_ERROR(TabletMetaManager::del_vector_iterate(
            _tablet.data_dir()->get_meta(), _tablet.tablet_id(), 0, UINT32_MAX,
            [&](uint32_t segment_id, int64_t version, std::string_view value) -> bool {
                auto iter = del_vector_cardinality_by_rssid.find(segment_id);
                if (iter == del_vector_cardinality_by_rssid.end()) {
                    return true;
                }
                if (iter->second == -1) {
                    DelVectorPtr delvec = std::make_shared<DelVector>();
                    if (!delvec->load(version, value.data(), value.size()).ok()) {
                        return false;
                    }
                    iter->second = delvec->cardinality();
                }
                return true;
            }));

    for (auto& [rsid, rowset] : _rowsets) {
        auto stats = std::make_unique<RowsetStats>();
        stats->num_segments = rowset->num_segments();
        stats->num_rows = rowset->num_rows();
        stats->byte_size = rowset->data_disk_size();
        stats->num_dels = 0;
        stats->partial_update_by_column = rowset->is_column_mode_partial_update();
        // the unapplied rowsets have no delete vector yet, so we only need to check the applied rowsets
        if (unapplied_rowsets.find(rsid) == unapplied_rowsets.end()) {
            for (int i = 0; i < rowset->num_segments(); i++) {
                auto itr = del_vector_cardinality_by_rssid.find(rsid + i);
                if (itr != del_vector_cardinality_by_rssid.end() && itr->second != -1) {
                    stats->num_dels += itr->second;
                } else {
                    std::string msg = strings::Substitute("delvec not found for rowset $0 segment $1", rsid, i);
                    LOG(ERROR) << msg;
                    return Status::InternalError(msg);
                }
            }
        }
        DCHECK_LE(stats->num_dels, stats->num_rows) << " tabletid:" << _tablet.tablet_id() << " rowset:" << rsid;
        _calc_compaction_score(stats.get());
        std::lock_guard lg(_rowset_stats_lock);
        _rowset_stats.emplace(rsid, std::move(stats));
    }
    del_vector_cardinality_by_rssid.clear();

    l2.unlock(); // _rowsets_lock

    RETURN_IF_ERROR(_load_pending_rowsets());

    _update_total_stats(_edit_version_infos[_apply_version_idx]->rowsets, nullptr, nullptr);
    VLOG(1) << "load tablet " << _debug_string(false, true);
    _try_commit_pendings_unlocked();
    _check_for_apply();

    return Status::OK();
}

size_t TabletUpdates::data_size() const {
    string err_rowsets;
    int64_t total_size = 0;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet deleted when call data_size() tablet:" << _tablet.tablet_id();
            return 0;
        }
        std::lock_guard lg(_rowset_stats_lock);
        auto& last = _edit_version_infos.back();
        for (uint32_t rowsetid : last->rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr != _rowset_stats.end()) {
                total_size += itr->second->byte_size;
            } else {
                StringAppendF(&err_rowsets, "%u,", rowsetid);
            }
        }
    }
    if (!err_rowsets.empty()) {
        LOG_EVERY_N(WARNING, 10) << "data_size() some rowset stats not found tablet=" << _tablet.tablet_id()
                                 << " rowset=" << err_rowsets;
    }
    return total_size;
}

size_t TabletUpdates::num_rows() const {
    string err_rowsets;
    int64_t total_row = 0;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet delete when call num_rows tablet:" << _tablet.tablet_id();
            return 0;
        }
        std::lock_guard lg(_rowset_stats_lock);
        auto& last = _edit_version_infos.back();
        for (uint32_t rowsetid : last->rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr != _rowset_stats.end()) {
                total_row += itr->second->num_rows;
            } else {
                StringAppendF(&err_rowsets, "%u,", rowsetid);
            }
        }
    }
    if (!err_rowsets.empty()) {
        LOG_EVERY_N(WARNING, 10) << "data_size() some rowset stats not found tablet=" << _tablet.tablet_id()
                                 << " rowset=" << err_rowsets;
    }
    return total_row;
}

std::pair<int64_t, int64_t> TabletUpdates::num_rows_and_data_size() const {
    string err_rowsets;
    int64_t total_row = 0;
    int64_t total_size = 0;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet deleted when call data_size() tablet:" << _tablet.tablet_id();
            return {total_row, total_size};
        }
        std::lock_guard lg(_rowset_stats_lock);
        auto& last = _edit_version_infos.back();
        for (uint32_t rowsetid : last->rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr != _rowset_stats.end()) {
                total_row += itr->second->num_rows;
                total_size += itr->second->byte_size;
            } else {
                StringAppendF(&err_rowsets, "%u,", rowsetid);
            }
        }
    }
    if (!err_rowsets.empty()) {
        LOG_EVERY_N(WARNING, 10) << "data_size() some rowset stats not found tablet=" << _tablet.tablet_id()
                                 << " rowset=" << err_rowsets;
    }
    return {total_row, total_size};
}

size_t TabletUpdates::num_rowsets() const {
    std::lock_guard rl(_lock);
    return _edit_version_infos.empty() ? 0 : _edit_version_infos.back()->rowsets.size();
}

size_t TabletUpdates::version_count() const {
    std::lock_guard rl(_lock);
    size_t ret = _pending_commits.size();
    if (!_edit_version_infos.empty()) {
        ret += _edit_version_infos.back()->rowsets.size();
    }
    return ret;
}

size_t TabletUpdates::num_pending() const {
    std::lock_guard rl(_lock);
    return _pending_commits.size();
}

int64_t TabletUpdates::max_version() const {
    std::lock_guard rl(_lock);
    return _edit_version_infos.empty() ? 0 : _edit_version_infos.back()->version.major();
}

Status TabletUpdates::get_rowsets_total_stats(const std::vector<uint32_t>& rowsets, size_t* total_rows,
                                              size_t* total_dels) {
    string err_rowsets;
    std::lock_guard lg(_rowset_stats_lock);
    for (auto rowsetid : rowsets) {
        auto itr = _rowset_stats.find(rowsetid);
        if (itr != _rowset_stats.end()) {
            *total_rows += itr->second->num_rows;
            *total_dels += itr->second->num_dels;
        } else {
            StringAppendF(&err_rowsets, "%u,", rowsetid);
        }
    }
    if (!err_rowsets.empty()) {
        string msg = strings::Substitute("get_rowset_total_stats() some rowset stats not found tablet:$0 rowsets:$1",
                                         _tablet.tablet_id(), err_rowsets);
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    return Status::OK();
}

void TabletUpdates::_sync_apply_version_idx(const EditVersion& edit_version) {
    // usually applied version is at the end of _edit_version_infos vector
    // so search from the back
    // assuming _lock held
    for (ssize_t i = _edit_version_infos.size() - 1; i >= 0; i--) {
        if (_edit_version_infos[i]->version == edit_version) {
            _apply_version_idx = i;
            _apply_version_changed.notify_all();
            return;
        }
    }
    std::string msg = strings::Substitute("illegal state, apply version not found in versions tablet:$0 $1",
                                          _tablet.tablet_id(), edit_version.to_string());
    LOG(ERROR) << msg;
    _set_error(msg);
}

void TabletUpdates::_redo_edit_version_log(const EditVersionMetaPB& edit_version_meta_pb) {
    std::unique_ptr<EditVersionInfo> edit_version_info = std::make_unique<EditVersionInfo>();
    edit_version_info->version =
            EditVersion(edit_version_meta_pb.version().major(), edit_version_meta_pb.version().minor());
    edit_version_info->creation_time = edit_version_meta_pb.creation_time();
    if (edit_version_meta_pb.rowsets_add_size() > 0 || edit_version_meta_pb.rowsets_del_size() > 0) {
        // incremental
        CHECK(!_edit_version_infos.empty()) << "incremental edit without full last version";
        auto& last_rowsets = _edit_version_infos.back()->rowsets;
        auto new_rowsets = modify(last_rowsets, edit_version_meta_pb.rowsets_add().begin(),
                                  edit_version_meta_pb.rowsets_add().end(), edit_version_meta_pb.rowsets_del().begin(),
                                  edit_version_meta_pb.rowsets_del().end());
        edit_version_info->rowsets.swap(new_rowsets);
    } else {
        // full
        edit_version_info->rowsets.assign(edit_version_meta_pb.rowsets().begin(), edit_version_meta_pb.rowsets().end());
    }
    edit_version_info->deltas.assign(edit_version_meta_pb.deltas().begin(), edit_version_meta_pb.deltas().end());
    if (edit_version_meta_pb.has_compaction()) {
        edit_version_info->compaction = std::make_unique<CompactionInfo>();
        auto& compaction_info_pb = edit_version_meta_pb.compaction();
        edit_version_info->compaction->start_version =
                EditVersion(compaction_info_pb.start_version().major(), compaction_info_pb.start_version().minor());
        edit_version_info->compaction->inputs.assign(compaction_info_pb.inputs().begin(),
                                                     compaction_info_pb.inputs().end());
        edit_version_info->compaction->output = compaction_info_pb.outputs()[0];
    }
    _edit_version_infos.emplace_back(std::move(edit_version_info));
    _next_rowset_id += edit_version_meta_pb.rowsetid_add();
}

Status TabletUpdates::get_apply_version_and_rowsets(int64_t* version, std::vector<RowsetSharedPtr>* rowsets,
                                                    std::vector<uint32_t>* rowset_ids) {
    std::lock_guard rl(_lock);
    if (_edit_version_infos.empty()) {
        string msg =
                strings::Substitute("tablet deleted when get_apply_version_and_rowsets tablet:$0", _tablet.tablet_id());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    EditVersionInfo* edit_version_info = nullptr;
    edit_version_info = _edit_version_infos[_apply_version_idx].get();
    rowsets->reserve(edit_version_info->rowsets.size());
    std::lock_guard<std::mutex> lg(_rowsets_lock);
    for (uint32_t rsid : edit_version_info->rowsets) {
        auto itr = _rowsets.find(rsid);
        DCHECK(itr != _rowsets.end());
        if (itr != _rowsets.end()) {
            rowsets->emplace_back(itr->second);
        } else {
            return Status::NotFound(
                    strings::Substitute("get_apply_version_and_rowsets rowset not found: version:$0 rowset:$1 $2",
                                        version, rsid, _debug_string(false, true)));
        }
    }
    rowset_ids->assign(edit_version_info->rowsets.begin(), edit_version_info->rowsets.end());
    *version = edit_version_info->version.major();
    return Status::OK();
}

Status TabletUpdates::rowset_commit(int64_t version, const RowsetSharedPtr& rowset, uint32_t wait_time) {
    auto span = Tracer::Instance().start_trace("rowset_commit");
    auto scope_span = trace::Scope(span);
    if (_error) {
        return Status::InternalError(
                strings::Substitute("rowset_commit failed, tablet updates is in error state: tablet:$0 $1",
                                    _tablet.tablet_id(), _error_msg));
    }
    Status st;
    {
        std::unique_lock<std::mutex> ul(_lock);
        if (_edit_version_infos.empty()) {
            string msg = strings::Substitute("tablet deleted when rowset_commit tablet:$0", _tablet.tablet_id());
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        if (version <= _edit_version_infos.back()->version.major()) {
            LOG(WARNING) << "ignored already committed version " << version << " of tablet " << _tablet.tablet_id()
                         << " txn_id: " << rowset->txn_id();
            _ignore_rowset_commit(version, rowset);
            return Status::OK();
        } else if (version > _edit_version_infos.back()->version.major() + 1) {
            if (_pending_commits.size() >= config::tablet_max_pending_versions) {
                // there must be something wrong, return error rather than accepting more commits
                string msg = strings::Substitute(
                        "rowset commit failed too many pending rowsets tablet:$0 version:$1 txn_id: $2 #pending:$3",
                        _tablet.tablet_id(), version, rowset->txn_id(), _pending_commits.size());
                LOG(WARNING) << msg;
                return Status::InternalError(msg);
            }
            if (!_pending_commits.emplace(version, rowset).second) {
                LOG(WARNING) << "ignore add rowset to pending commits, same version already exists version:" << version
                             << " txn_id: " << rowset->txn_id() << " " << _debug_string(false, false);
                _ignore_rowset_commit(version, rowset);
            } else {
                st = TabletMetaManager::pending_rowset_commit(
                        _tablet.data_dir(), _tablet.tablet_id(), version, rowset->rowset_meta()->get_meta_pb(),
                        RowsetMetaManager::get_rowset_meta_key(_tablet.tablet_uid(), rowset->rowset_id()));
                if (!st.ok()) {
                    LOG(WARNING) << "add rowset to pending commits failed tablet:" << _tablet.tablet_id()
                                 << " version:" << version << " txn_id: " << rowset->txn_id() << " " << st << " "
                                 << _debug_string(false, true);
                    return st;
                }
                LOG(INFO) << "add rowset to pending commits tablet:" << _tablet.tablet_id() << " version:" << version
                          << " txn_id: " << rowset->txn_id() << " #pending:" << _pending_commits.size();
            }
            return Status::OK();
        }
        st = _rowset_commit_unlocked(version, rowset);
        if (st.ok()) {
            LOG(INFO) << "commit rowset tablet:" << _tablet.tablet_id() << " version:" << version
                      << " txn_id: " << rowset->txn_id() << " " << rowset->rowset_id().to_string()
                      << " rowset:" << rowset->rowset_meta()->get_rowset_seg_id() << " #seg:" << rowset->num_segments()
                      << " #delfile:" << rowset->num_delete_files() << " #uptfile:" << rowset->num_update_files()
                      << " #row:" << rowset->num_rows()
                      << " size:" << PrettyPrinter::print(rowset->data_disk_size(), TUnit::BYTES)
                      << " #pending:" << _pending_commits.size();
            _try_commit_pendings_unlocked();
            _check_for_apply();
            if (wait_time > 0) {
                st = _wait_for_version(EditVersion(version, 0), wait_time, ul);
            }
        }
    }
    if (!st.ok() && !st.is_time_out()) {
        LOG(WARNING) << "rowset commit failed tablet:" << _tablet.tablet_id() << " version:" << version
                     << " txn_id: " << rowset->txn_id() << " pending:" << _pending_commits.size() << " msg:" << st;
    } else if (st.is_time_out()) {
        st = Status::OK();
    }
    return st;
}

Status TabletUpdates::_rowset_commit_unlocked(int64_t version, const RowsetSharedPtr& rowset) {
    auto span =
            Tracer::Instance().start_trace_txn_tablet("rowset_commit_unlocked", rowset->txn_id(), _tablet.tablet_id());
    span->SetAttribute("version", version);
    auto scoped = trace::Scope(span);
    EditVersionMetaPB edit;
    auto edit_version_pb = edit.mutable_version();
    edit_version_pb->set_major(version);
    edit_version_pb->set_minor(0);
    int64_t creation_time = time(nullptr);
    edit.set_creation_time(creation_time);
    std::vector<uint32_t> nrs;
    uint32_t rowsetid = _next_rowset_id;
    if (_edit_version_infos.empty()) {
        edit.mutable_rowsets()->Add(rowsetid);
        nrs.emplace_back(rowsetid);
    } else {
        auto& ors = _edit_version_infos.back()->rowsets;
        nrs.reserve(ors.size() + 1);
        nrs.assign(ors.begin(), ors.end());
        nrs.push_back(rowsetid);
        if (nrs.size() <= 16) {
            // full copy
            repeated_field_add(edit.mutable_rowsets(), nrs.begin(), nrs.end());
        } else {
            // incremental
            edit.add_rowsets_add(rowsetid);
        }
    }
    edit.add_deltas(rowsetid);
    uint32_t rowsetid_add = std::max(1U, (uint32_t)rowset->num_segments());
    edit.set_rowsetid_add(rowsetid_add);
    // TODO: is rollback modification of rowset meta required if commit failed?
    rowset->make_commit(version, rowsetid);
    span->AddEvent("save_meta_begin");
    auto st = TabletMetaManager::rowset_commit(
            _tablet.data_dir(), _tablet.tablet_id(), _next_log_id, &edit, rowset->rowset_meta()->get_meta_pb(),
            RowsetMetaManager::get_rowset_meta_key(_tablet.tablet_uid(), rowset->rowset_id()));
    span->AddEvent("save_meta_end");
    if (!st.ok()) {
        LOG(WARNING) << "rowset commit failed: " << st << " " << _debug_string(false, false);
        return st;
    }
    // apply in-memory state after commit success
    _next_log_id++;
    _next_rowset_id += rowsetid_add;
    std::unique_ptr<EditVersionInfo> edit_version_info = std::make_unique<EditVersionInfo>();
    edit_version_info->version = EditVersion(version, 0);
    edit_version_info->creation_time = creation_time;
    edit_version_info->rowsets.swap(nrs);
    edit_version_info->deltas.push_back(rowsetid);
    _edit_version_infos.emplace_back(std::move(edit_version_info));
    _check_creation_time_increasing();
    {
        std::lock_guard<std::mutex> lg(_rowsets_lock);
        _rowsets[rowsetid] = rowset;
    }
    // update stats of the newly added rowset
    {
        auto rowset_stats = std::make_unique<RowsetStats>();
        rowset_stats->num_segments = rowset->num_segments();
        rowset_stats->num_rows = rowset->num_rows();
        rowset_stats->num_dels = 0;
        rowset_stats->byte_size = rowset->data_disk_size();
        rowset_stats->row_size = rowset->total_row_size();
        rowset_stats->partial_update_by_column = rowset->is_column_mode_partial_update();
        _calc_compaction_score(rowset_stats.get());

        std::lock_guard lg(_rowset_stats_lock);
        _rowset_stats.emplace(rowsetid, std::move(rowset_stats));
    }
    VLOG(1) << "rowset commit finished: " << _debug_string(false, true);
    return Status::OK();
}

void TabletUpdates::_check_creation_time_increasing() {
    if (_edit_version_infos.size() >= 2) {
        auto last2 = _edit_version_infos[_edit_version_infos.size() - 2].get();
        auto last1 = _edit_version_infos[_edit_version_infos.size() - 1].get();
        if (last2->creation_time > last1->creation_time) {
            LOG(ERROR) << strings::Substitute("creation_time decreased tablet:$0 $1:$2 > $3:$4", _tablet.tablet_id(),
                                              last2->version.to_string(), last2->creation_time,
                                              last1->version.to_string(), last1->creation_time);
        }
    }
}

void TabletUpdates::_try_commit_pendings_unlocked() {
    if (_pending_commits.size() > 0) {
        int64_t current_version = _edit_version_infos.back()->version.major();
        for (auto itr = _pending_commits.begin(); itr != _pending_commits.end();) {
            int64_t version = itr->first;
            if (version <= current_version) {
                LOG(WARNING) << "ignore pending rowset tablet: " << _tablet.tablet_id() << " version:" << version
                             << " txn_id: " << itr->second->txn_id() << " #pending:" << _pending_commits.size();
                _ignore_rowset_commit(version, itr->second);
                auto st = TabletMetaManager::delete_pending_rowset(_tablet.data_dir(), _tablet.tablet_id(), version);
                LOG_IF(WARNING, !st.ok())
                        << "Failed to delete_pending_rowset tablet:" << _tablet.tablet_id() << " version:" << version
                        << " txn_id: " << itr->second->txn_id() << " rowset: " << itr->second->rowset_id().to_string();
                itr = _pending_commits.erase(itr);
            } else if (version == current_version + 1) {
                auto& rowset = itr->second;
                auto st = _rowset_commit_unlocked(version, rowset);
                if (!st.ok()) {
                    LOG(ERROR) << "commit rowset (pending) failed tablet: " << _tablet.tablet_id()
                               << " version:" << version << " txn_id: " << rowset->txn_id()
                               << " rowset:" << rowset->rowset_meta()->get_rowset_seg_id()
                               << " #seg:" << rowset->num_segments() << " #row:" << rowset->num_rows()
                               << " size:" << PrettyPrinter::print(rowset->data_disk_size(), TUnit::BYTES)
                               << " #pending:" << _pending_commits.size() << " " << st.to_string();
                    return;
                }
                LOG(INFO) << "commit rowset (pending) tablet:" << _tablet.tablet_id() << " version:" << version
                          << " txn_id: " << rowset->txn_id() << " rowset:" << rowset->rowset_meta()->get_rowset_seg_id()
                          << " #seg:" << rowset->num_segments() << " #row:" << rowset->num_rows()
                          << " size:" << PrettyPrinter::print(rowset->data_disk_size(), TUnit::BYTES)
                          << " #pending:" << _pending_commits.size();
                itr = _pending_commits.erase(itr);
                current_version = _edit_version_infos.back()->version.major();
            } else {
                break;
            }
        }
    }
}

void TabletUpdates::_ignore_rowset_commit(int64_t version, const RowsetSharedPtr& rowset) {
    auto st = RowsetMetaManager::remove(_tablet.data_dir()->get_meta(), _tablet.tablet_uid(), rowset->rowset_id());
    LOG_IF(WARNING, !st.ok()) << "Failed to remove rowset meta tablet:" << _tablet.tablet_id() << " version:" << version
                              << " txn_id: " << rowset->txn_id() << " rowset: " << rowset->rowset_id().to_string();
}

class ApplyCommitTask : public Runnable {
public:
    ApplyCommitTask(TabletSharedPtr tablet) : _tablet(std::move(tablet)) {}

    void run() override { _tablet->updates()->do_apply(); }

private:
    TabletSharedPtr _tablet;
};

void TabletUpdates::_check_for_apply() {
    // assuming _lock is already hold
    if (_apply_stopped) {
        return;
    }
    _apply_running_lock.lock();
    if (_apply_running || _apply_version_idx + 1 == _edit_version_infos.size()) {
        _apply_running_lock.unlock();
        return;
    }
    _apply_running = true;
    _apply_running_lock.unlock();
    std::shared_ptr<Runnable> task(
            std::make_shared<ApplyCommitTask>(std::static_pointer_cast<Tablet>(_tablet.shared_from_this())));
    auto st = StorageEngine::instance()->update_manager()->apply_thread_pool()->submit(std::move(task));
    if (!st.ok()) {
        std::string msg =
                strings::Substitute("submit apply task failed: $0 $1", st.to_string(), _debug_string(false, false));
        LOG(FATAL) << msg;
    }
}

void TabletUpdates::do_apply() {
    SCOPED_THREAD_LOCAL_CHECK_MEM_LIMIT_SETTER(false);
    // only 1 thread at max is running this method
    bool first = true;
    while (!_apply_stopped) {
        const EditVersionInfo* version_info_apply = nullptr;
        {
            std::lock_guard rl(_lock);
            if (_edit_version_infos.empty()) {
                LOG(WARNING) << "tablet deleted when doing apply tablet:" << _tablet.tablet_id();
                break;
            }
            if (_apply_version_idx + 1 >= _edit_version_infos.size()) {
                if (first) {
                    LOG(WARNING) << "illegal state: do_apply should not be called when there is "
                                    "nothing to apply: "
                                 << _debug_string(false);
                }
                break;
            }
            // we make sure version_info_apply will never be deleted before apply finished
            version_info_apply = _edit_version_infos[_apply_version_idx + 1].get();
        }
        if (version_info_apply->deltas.size() > 0) {
            int64_t duration_ns = 0;
            {
                StarRocksMetrics::instance()->update_rowset_commit_apply_total.increment(1);
                SCOPED_RAW_TIMER(&duration_ns);
                _apply_rowset_commit(*version_info_apply);
            }
            StarRocksMetrics::instance()->update_rowset_commit_apply_duration_us.increment(duration_ns / 1000);
        } else if (version_info_apply->compaction) {
            // _compaction_running may be false after BE restart, reset it to true
            _compaction_running = true;
            _apply_compaction_commit(*version_info_apply);
            _compaction_running = false;
        } else {
            std::string msg = strings::Substitute("bad EditVersionInfo tablet: $0 ", _tablet.tablet_id());
            LOG(ERROR) << msg;
            _set_error(msg);
        }
        first = false;
        if (_error) {
            break;
        }
    }
    std::lock_guard<std::mutex> lg(_apply_running_lock);
    CHECK(_apply_running) << "illegal state: _apply_running should be true";
    _apply_running = false;
    _apply_stopped_cond.notify_all();
}

void TabletUpdates::_stop_and_wait_apply_done() {
    _apply_stopped = true;
    std::unique_lock<std::mutex> ul(_apply_running_lock);
    while (_apply_running) {
        _apply_stopped_cond.wait(ul);
    }
}

Status TabletUpdates::get_latest_applied_version(EditVersion* latest_applied_version) {
    std::lock_guard l(_lock);
    if (_edit_version_infos.empty()) {
        string msg =
                strings::Substitute("tablet deleted when get_latest_applied_version tablet:$0", _tablet.tablet_id());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    *latest_applied_version = _edit_version_infos[_apply_version_idx]->version;
    return Status::OK();
}

void TabletUpdates::_apply_column_partial_update_commit(const EditVersionInfo& version_info,
                                                        const RowsetSharedPtr& rowset) {
    auto span = Tracer::Instance().start_trace_tablet("apply_column_partial_update_commit", _tablet.tablet_id());
    auto scoped = trace::Scope(span);

    auto tablet_id = _tablet.tablet_id();
    auto& version = version_info.version;
    auto manager = StorageEngine::instance()->update_manager();

    span->SetAttribute("txn_id", rowset->txn_id());
    span->SetAttribute("version", version.major());

    // 1. load updates in rowset, prepare state for generating delta column group later.
    auto state_entry = manager->update_column_state_cache().get_or_create(
            strings::Substitute("$0_$1", tablet_id, rowset->rowset_id().to_string()));
    state_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    // when failure happen, remove state cache and record error msg
    auto failure_handler = [&](const std::string& str, const Status& st) {
        manager->update_column_state_cache().remove(state_entry);
        std::string msg = strings::Substitute("$0: $1 $2", str, st.to_string(), debug_string());
        LOG(ERROR) << msg;
        _set_error(msg);
    };
    auto& state = state_entry->value();
    auto st = state.load(&_tablet, rowset.get(), manager->mem_tracker());
    manager->update_column_state_cache().update_object_size(state_entry, state.memory_usage());
    if (!st.ok()) {
        failure_handler("apply_column_partial_rowset_commit error: load rowset update state failed", st);
        return;
    }

    std::lock_guard lg(_index_lock);
    // 2. load primary index, using it in finalize step.
    auto index_entry = manager->index_cache().get_or_create(tablet_id);
    index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    auto& index = index_entry->value();
    // empty rowset does not need to load in-memory primary index, so skip it
    if (rowset->has_data_files() || _tablet.get_enable_persistent_index()) {
        auto st = index.load(&_tablet);
        manager->index_cache().update_object_size(index_entry, index.memory_usage());
        if (!st.ok()) {
            manager->index_cache().remove(index_entry);
            failure_handler("load primary index failed", st);
            return;
        }
    }

    span->AddEvent("gen_delta_column_group");
    // 3. finalize and generate delta column group
    st = state.finalize(&_tablet, rowset.get(), index);
    if (!st.ok()) {
        failure_handler("finalize failed", st);
        return;
    }

    // 4. write meta and make it apply.
    {
        std::lock_guard wl(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet deleted when apply rowset commmit tablet:" << tablet_id;
            return;
        }

        st = TabletMetaManager::apply_rowset_commit(_tablet.data_dir(), tablet_id, _next_log_id, version,
                                                    state.delta_column_groups(),
                                                    &(rowset->rowset_meta()->get_meta_pb()));

        if (!st.ok()) {
            failure_handler("apply_rowset_commit failed", st);
            return;
        }
        // set cached delta column group
        for (const auto& dcg : state.delta_column_groups()) {
            st = manager->set_cached_delta_column_group(_tablet.data_dir()->get_meta(),
                                                        TabletSegmentId(tablet_id, dcg.first), dcg.second);
            if (!st.ok()) {
                failure_handler("set_cached_delta_column_group failed", st);
                return;
            }
        }
        // 5. apply memory
        _next_log_id++;
        _apply_version_idx++;
        _apply_version_changed.notify_all();
    }

    // 6. clear state and index cache
    manager->update_column_state_cache().remove(state_entry);
    if (index.enable_persistent_index() ^ _tablet.get_enable_persistent_index()) {
        manager->index_cache().remove(index_entry);
    } else {
        manager->index_cache().release(index_entry);
    }
}

void TabletUpdates::_apply_rowset_commit(const EditVersionInfo& version_info) {
    uint32_t rowset_id = version_info.deltas[0];
    RowsetSharedPtr rowset = _get_rowset(rowset_id);
    if (rowset->is_column_mode_partial_update()) {
        StarRocksMetrics::instance()->column_partial_update_apply_total.increment(1);
        int64_t duration_ns = 0;
        {
            SCOPED_RAW_TIMER(&duration_ns);
            _apply_column_partial_update_commit(version_info, rowset);
        }
        StarRocksMetrics::instance()->column_partial_update_apply_duration_us.increment(duration_ns / 1000);
    } else {
        _apply_normal_rowset_commit(version_info, rowset);
    }
}

// check if delta column generated from begin version to now.
bool TabletUpdates::check_delta_column_generate_from_version(EditVersion begin_version) {
    // check edit version info from latest to begin_version
    std::lock_guard rl(_lock);
    for (auto i = _edit_version_infos.rbegin(); i != _edit_version_infos.rend() && begin_version < (*i)->version; i++) {
        if ((*i)->deltas.size() != 0) {
            uint32_t rowset_id = (*i)->deltas[0];
            RowsetSharedPtr rowset = _get_rowset(rowset_id);
            if (rowset->is_column_mode_partial_update()) {
                LOG(INFO) << "delta column group is generated in tablet_id: " << _tablet.tablet_id()
                          << " version: " << (*i)->version;
                return true;
            }
        }
    }
    return false;
}

void TabletUpdates::_apply_normal_rowset_commit(const EditVersionInfo& version_info, const RowsetSharedPtr& rowset) {
    auto span = Tracer::Instance().start_trace_tablet("apply_rowset_commit", _tablet.tablet_id());
    auto scoped = trace::Scope(span);

    // NOTE: after commit, apply must success or fatal crash
    int64_t t_start = MonotonicMillis();
    auto tablet_id = _tablet.tablet_id();
    uint32_t rowset_id = version_info.deltas[0];
    auto& version = version_info.version;

    auto manager = StorageEngine::instance()->update_manager();

    span->SetAttribute("txn_id", rowset->txn_id());
    span->SetAttribute("version", version.major());

    // 1. load upserts/deletes in rowset
    auto state_entry = manager->update_state_cache().get_or_create(
            strings::Substitute("$0_$1", tablet_id, rowset->rowset_id().to_string()));
    state_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    auto& state = state_entry->value();
    auto st = state.load(&_tablet, rowset.get());
    manager->update_state_cache().update_object_size(state_entry, state.memory_usage());
    if (!st.ok()) {
        manager->update_state_cache().remove(state_entry);
        std::string msg = strings::Substitute("_apply_rowset_commit error: load rowset update state failed: $0 $1",
                                              st.to_string(), debug_string());
        LOG(ERROR) << msg;
        _set_error(msg);
        return;
    }

    std::lock_guard lg(_index_lock);
    // 2. load index
    auto index_entry = manager->index_cache().get_or_create(tablet_id);
    index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    auto& index = index_entry->value();
    // empty rowset does not need to load in-memory primary index, so skip it
    if (rowset->has_data_files() || _tablet.get_enable_persistent_index()) {
        auto st = index.load(&_tablet);
        manager->index_cache().update_object_size(index_entry, index.memory_usage());
        if (!st.ok()) {
            manager->index_cache().remove(index_entry);
            std::string msg = strings::Substitute("_apply_rowset_commit error: load primary index failed: $0 $1",
                                                  st.to_string(), debug_string());
            LOG(ERROR) << msg;
            _set_error(msg);
            return;
        }
    }
    // `enable_persistent_index` of tablet maybe change by alter, we should get `enable_persistent_index` from index to
    // avoid inconsistency between persistent index file and PersistentIndexMeta
    bool enable_persistent_index = index.enable_persistent_index();
    size_t merge_num = 0;
    {
        std::lock_guard lg(_rowset_stats_lock);
        auto iter = _rowset_stats.find(rowset_id);
        if (iter == _rowset_stats.end()) {
            string msg = strings::Substitute("inconsistent rowset_stats, rowset not found tablet=$0 rowsetid=$1",
                                             _tablet.tablet_id(), rowset_id);
            DCHECK(false) << msg;
            LOG(ERROR) << msg;
            _set_error(msg);
            return;
        } else {
            size_t num_adds = iter->second->num_rows;
            size_t num_dels = iter->second->num_dels;
            merge_num = num_adds + num_dels;
        }
    }
    st = index.prepare(version, merge_num);
    if (!st.ok()) {
        manager->index_cache().remove(index_entry);
        std::string msg = strings::Substitute("_apply_rowset_commit error: primary index prepare failed: $0 $1",
                                              st.to_string(), debug_string());
        LOG(ERROR) << msg;
        _set_error(msg);
        return;
    }

    int64_t t_apply = MonotonicMillis();
    int32_t conditional_column = -1;
    const auto& txn_meta = rowset->rowset_meta()->get_meta_pb().txn_meta();
    if (txn_meta.has_merge_condition()) {
        for (int i = 0; i < _tablet.tablet_schema().columns().size(); ++i) {
            if (_tablet.tablet_schema().column(i).name() == txn_meta.merge_condition()) {
                conditional_column = i;
                break;
            }
        }
    }

    span->AddEvent("update_index");
    // 3. generate delvec
    // add initial empty delvec for new segments
    PrimaryIndex::DeletesMap new_deletes;
    size_t delete_op = 0;
    for (uint32_t i = 0; i < rowset->num_segments(); i++) {
        new_deletes[rowset_id + i] = {};
    }
    EditVersion latest_applied_version;
    st = get_latest_applied_version(&latest_applied_version);

    int64_t full_row_size = 0;
    int64_t full_rowset_size = 0;
    if (rowset->rowset_meta()->get_meta_pb().delfile_idxes_size() == 0) {
        for (uint32_t i = 0; i < rowset->num_segments(); i++) {
            state.load_upserts(rowset.get(), i);
            auto& upserts = state.upserts();
            if (upserts[i] != nullptr) {
                // used for auto increment delete-partial update conflict
                std::unique_ptr<Column> delete_pks = nullptr;
                // apply partial rowset segment
                st = state.apply(&_tablet, rowset.get(), rowset_id, i, latest_applied_version, index, delete_pks,
                                 &full_row_size);
                if (!st.ok()) {
                    manager->update_state_cache().remove(state_entry);
                    std::string msg =
                            strings::Substitute("_apply_rowset_commit error: apply rowset update state failed: $0 $1",
                                                st.to_string(), debug_string());
                    LOG(ERROR) << msg;
                    _set_error(msg);
                    return;
                }
                st = _do_update(rowset_id, i, conditional_column, latest_applied_version.major(), upserts, index,
                                tablet_id, &new_deletes);
                if (!st.ok()) {
                    manager->update_state_cache().remove(state_entry);
                    std::string msg =
                            strings::Substitute("_apply_rowset_commit error: apply rowset update state failed: $0 $1",
                                                st.to_string(), debug_string());
                    LOG(ERROR) << msg;
                    _set_error(msg);
                    return;
                }
                manager->index_cache().update_object_size(index_entry, index.memory_usage());
                if (delete_pks != nullptr) {
                    index.erase(*delete_pks, &new_deletes);
                }
            }
            state.release_upserts(i);
        }

        // two states
        // 1. upgrade from old version. delfile_idxes in rowset meta is empty, we still need to load delete files
        // 2. pure upsert. no delete files, the following logic will be skip
        for (uint32_t i = 0; i < rowset->num_delete_files(); i++) {
            state.load_deletes(rowset.get(), i);
            auto& deletes = state.deletes();
            delete_op += deletes[i]->size();
            index.erase(*deletes[i], &new_deletes);
            state.release_deletes(i);
        }
    } else {
        uint32_t delfile_num = rowset->rowset_meta()->get_meta_pb().delfile_idxes_size();
        uint32_t upsert_num = rowset->num_segments();
        DCHECK(rowset->num_delete_files() == delfile_num);

        uint32_t loaded_delfile = 0;
        uint32_t loaded_upsert = 0;
        uint32_t i = 0;
        while (i < delfile_num + upsert_num) {
            uint32_t del_idx = delfile_num + upsert_num;
            if (loaded_delfile < delfile_num) {
                del_idx = rowset->rowset_meta()->get_meta_pb().delfile_idxes(loaded_delfile);
            }
            while (i < del_idx) {
                state.load_upserts(rowset.get(), loaded_upsert);
                auto& upserts = state.upserts();
                if (upserts[loaded_upsert] != nullptr) {
                    // used for auto increment delete-partial update conflict
                    std::unique_ptr<Column> delete_pks = nullptr;
                    // apply partial rowset segment
                    st = state.apply(&_tablet, rowset.get(), rowset_id, loaded_upsert, latest_applied_version, index,
                                     delete_pks, &full_row_size);
                    if (!st.ok()) {
                        manager->update_state_cache().remove(state_entry);
                        std::string msg = strings::Substitute(
                                "_apply_rowset_commit error: apply rowset update state failed: $0 $1", st.to_string(),
                                debug_string());
                        LOG(ERROR) << msg;
                        _set_error(msg);
                        return;
                    }
                    st = _do_update(rowset_id, loaded_upsert, conditional_column, latest_applied_version.major(),
                                    upserts, index, tablet_id, &new_deletes);
                    if (!st.ok()) {
                        manager->update_state_cache().remove(state_entry);
                        std::string msg = strings::Substitute(
                                "_apply_rowset_commit error: apply rowset update state failed: $0 $1", st.to_string(),
                                debug_string());
                        LOG(ERROR) << msg;
                        _set_error(msg);
                        return;
                    }
                    manager->index_cache().update_object_size(index_entry, index.memory_usage());
                    if (delete_pks != nullptr) {
                        index.erase(*delete_pks, &new_deletes);
                    }
                }
                i++;
                loaded_upsert++;
                state.release_upserts(i);
            }
            if (loaded_delfile < delfile_num) {
                DCHECK(i == del_idx);
                state.load_deletes(rowset.get(), loaded_delfile);
                auto& deletes = state.deletes();
                delete_op += deletes[loaded_delfile]->size();
                index.erase(*deletes[loaded_delfile], &new_deletes);
                state.release_deletes(loaded_delfile);
                i++;
                loaded_delfile++;
            }
        }
    }
    full_row_size += rowset->rowset_meta()->total_row_size();
    full_rowset_size = rowset->total_segment_data_size();

    PersistentIndexMetaPB index_meta;
    if (enable_persistent_index) {
        st = TabletMetaManager::get_persistent_index_meta(_tablet.data_dir(), tablet_id, &index_meta);
        if (!st.ok() && !st.is_not_found()) {
            std::string msg = strings::Substitute("get persistent index meta failed: $0", st.to_string());
            LOG(ERROR) << msg << " " << _debug_string(false, true);
            _set_error(msg);
            return;
        }
    }
    span->AddEvent("commit_index");
    st = index.commit(&index_meta);
    if (!st.ok()) {
        std::string msg = strings::Substitute("primary index commit failed: $0", st.to_string());
        LOG(ERROR) << msg << " " << _debug_string(false, true);
        _set_error(msg);
        return;
    }

    manager->index_cache().update_object_size(index_entry, index.memory_usage());
    // release resource
    // update state only used once, so delete it
    manager->update_state_cache().remove(state_entry);
    int64_t t_index = MonotonicMillis();

    span->AddEvent("gen_delvec");
    size_t ndelvec = new_deletes.size();
    vector<std::pair<uint32_t, DelVectorPtr>> new_del_vecs(ndelvec);
    size_t idx = 0;
    size_t old_total_del = 0;
    size_t new_del = 0;
    size_t total_del = 0;
    string delvec_change_info;
    for (auto& new_delete : new_deletes) {
        uint32_t rssid = new_delete.first;
        if (rssid >= rowset_id && rssid < rowset_id + rowset->num_segments()) {
            // it's newly added rowset's segment, do not have latest delvec yet
            new_del_vecs[idx].first = rssid;
            new_del_vecs[idx].second = std::make_shared<DelVector>();
            auto& del_ids = new_delete.second;
            new_del_vecs[idx].second->init(version.major(), del_ids.data(), del_ids.size());
            if (VLOG_IS_ON(1)) {
                StringAppendF(&delvec_change_info, " %u:+%zu", rssid, del_ids.size());
            }
            new_del += del_ids.size();
            total_del += del_ids.size();
        } else {
            TabletSegmentId tsid;
            tsid.tablet_id = tablet_id;
            tsid.segment_id = rssid;
            DelVectorPtr old_del_vec;
            // TODO(cbl): should get the version before this apply version, to be safe
            st = manager->get_latest_del_vec(_tablet.data_dir()->get_meta(), tsid, &old_del_vec);
            if (!st.ok()) {
                std::string msg = strings::Substitute("_apply_rowset_commit error: get_latest_del_vec failed: $0 $1",
                                                      st.to_string(), debug_string());
                LOG(ERROR) << msg;
                _set_error(msg);
                return;
            }
            new_del_vecs[idx].first = rssid;
            old_del_vec->add_dels_as_new_version(new_delete.second, version.major(), &(new_del_vecs[idx].second));
            size_t cur_old = old_del_vec->cardinality();
            size_t cur_add = new_delete.second.size();
            size_t cur_new = new_del_vecs[idx].second->cardinality();
            if (cur_old + cur_add != cur_new) {
                // should not happen, data inconsistent
                LOG(FATAL) << strings::Substitute(
                        "delvec inconsistent tablet:$0 rssid:$1 #old:$2 #add:$3 #new:$4 old_v:$5 "
                        "v:$6",
                        _tablet.tablet_id(), rssid, cur_old, cur_add, cur_new, old_del_vec->version(), version.major());
            }
            if (VLOG_IS_ON(1)) {
                StringAppendF(&delvec_change_info, " %u:%zu(%ld)+%zu=%zu", rssid, cur_old, old_del_vec->version(),
                              cur_add, cur_new);
            }
            old_total_del += cur_old;
            new_del += cur_add;
            total_del += cur_new;
        }

        idx++;

        // Update the stats of affected rowsets.
        std::lock_guard lg(_rowset_stats_lock);
        auto iter = _rowset_stats.upper_bound(rssid);
        iter--;
        if (iter == _rowset_stats.end()) {
            string msg = strings::Substitute("inconsistent rowset_stats, rowset not found tablet=$0 rssid=$1 $2",
                                             _tablet.tablet_id(), rssid);
            DCHECK(false) << msg;
            LOG(ERROR) << msg;
        } else if (rssid >= iter->first + iter->second->num_segments) {
            string msg = strings::Substitute("inconsistent rowset_stats, tablet=$0 rssid=$1 >= $2", _tablet.tablet_id(),
                                             rssid, iter->first + iter->second->num_segments);
            DCHECK(false) << msg;
            LOG(ERROR) << msg;
        } else {
            iter->second->num_dels += new_delete.second.size();
            _calc_compaction_score(iter->second.get());
            DCHECK_LE(iter->second->num_dels, iter->second->num_rows);
        }
    }
    new_deletes.clear();
    StarRocksMetrics::instance()->update_del_vector_deletes_total.increment(total_del);
    StarRocksMetrics::instance()->update_del_vector_deletes_new.increment(new_del);
    int64_t t_delvec = MonotonicMillis();

    {
        std::lock_guard wl(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet deleted when apply rowset commmit tablet:" << tablet_id;
            return;
        }
        // 4. write meta
        const auto& rowset_meta_pb = rowset->rowset_meta()->get_meta_pb();
        if (rowset_meta_pb.has_txn_meta()) {
            full_rowset_size = rowset->total_segment_data_size();
            rowset->rowset_meta()->clear_txn_meta();
            rowset->rowset_meta()->set_total_row_size(full_row_size);
            rowset->rowset_meta()->set_total_disk_size(full_rowset_size);
            rowset->rowset_meta()->set_data_disk_size(full_rowset_size);
            st = TabletMetaManager::apply_rowset_commit(_tablet.data_dir(), tablet_id, _next_log_id, version,
                                                        new_del_vecs, index_meta, enable_persistent_index,
                                                        &(rowset->rowset_meta()->get_meta_pb()));
        } else {
            st = TabletMetaManager::apply_rowset_commit(_tablet.data_dir(), tablet_id, _next_log_id, version,
                                                        new_del_vecs, index_meta, enable_persistent_index, nullptr);
        }

        if (!st.ok()) {
            std::string msg = strings::Substitute("_apply_rowset_commit error: write meta failed: $0 $1",
                                                  st.to_string(), _debug_string(false));
            LOG(ERROR) << msg;
            _set_error(msg);
            return;
        }
        // put delvec in cache
        TabletSegmentId tsid;
        tsid.tablet_id = tablet_id;
        for (auto& delvec_pair : new_del_vecs) {
            tsid.segment_id = delvec_pair.first;
            manager->set_cached_del_vec(tsid, delvec_pair.second);
            // try to set empty dcg cache, for improving latency when reading
            manager->set_cached_empty_delta_column_group(_tablet.data_dir()->get_meta(), tsid);
        }
        // 5. apply memory
        _next_log_id++;
        _apply_version_idx++;
        _apply_version_changed.notify_all();
    }

    {
        std::lock_guard lg(_rowset_stats_lock);
        auto iter = _rowset_stats.find(rowset_id);
        if (iter == _rowset_stats.end()) {
            string msg = strings::Substitute("inconsistent rowset_stats, rowset not found tablet=$0 rowsetid=$1",
                                             _tablet.tablet_id(), rowset_id);
            LOG(ERROR) << msg;
        } else {
            iter->second->byte_size = full_rowset_size;
            iter->second->row_size = full_row_size;
        }
    }

    st = index.on_commited();
    if (!st.ok()) {
        std::string msg = strings::Substitute("primary index on_commit failed: $0", st.to_string());
        LOG(ERROR) << msg;
        _set_error(msg);
        return;
    }

    // if `enable_persistent_index` of tablet is change(maybe changed by alter table)
    // we should try to remove the index_entry from cache
    // Otherwise index may be used for later commits, keep in cache
    if (enable_persistent_index ^ _tablet.get_enable_persistent_index()) {
        manager->index_cache().remove(index_entry);
    } else {
        manager->index_cache().release(index_entry);
    }
    _update_total_stats(version_info.rowsets, nullptr, nullptr);
    int64_t t_write = MonotonicMillis();

    size_t del_percent = _cur_total_rows == 0 ? 0 : (_cur_total_dels * 100) / _cur_total_rows;
    LOG(INFO) << "apply_rowset_commit finish. tablet:" << tablet_id << " version:" << version_info.version.to_string()
              << " txn_id: " << rowset->txn_id() << " total del/row:" << _cur_total_dels << "/" << _cur_total_rows
              << " " << del_percent << "% rowset:" << rowset_id << " #seg:" << rowset->num_segments()
              << " #op(upsert:" << rowset->num_rows() << " del:" << delete_op << ") #del:" << old_total_del << "+"
              << new_del << "=" << total_del << " #dv:" << ndelvec << " duration:" << t_write - t_start << "ms"
              << strings::Substitute("($0/$1/$2/$3)", t_apply - t_start, t_index - t_apply, t_delvec - t_index,
                                     t_write - t_delvec);
    VLOG(1) << "rowset commit apply " << delvec_change_info << " " << _debug_string(true, true);
}

RowsetSharedPtr TabletUpdates::_get_rowset(uint32_t rowset_id) {
    std::lock_guard<std::mutex> lg(_rowsets_lock);
    auto itr = _rowsets.find(rowset_id);
    if (itr == _rowsets.end()) {
        // TODO: _rowsets will act as a cache in the future
        // need to load rowset from rowsetdb, currently just return null
        return {};
    }
    return itr->second;
}

Status TabletUpdates::_wait_for_version(const EditVersion& version, int64_t timeout_ms,
                                        std::unique_lock<std::mutex>& ul) {
    if (_edit_version_infos.empty()) {
        string msg = strings::Substitute("tablet deleted when _wait_for_version tablet:$0", _tablet.tablet_id());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    if (!(_edit_version_infos[_apply_version_idx]->version < version)) {
        return Status::OK();
    }
    int64_t wait_start = MonotonicMillis();
    while (true) {
        if (_apply_stopped) {
            return Status::InternalError(strings::Substitute("wait_for_version version:$0 failed: apply stopped $1",
                                                             version.to_string(), _debug_string(false, true)));
        }
        _apply_version_changed.wait_for(ul, std::chrono::seconds(2));
        if (_error) {
            break;
        }
        int64_t now = MonotonicMillis();
        if (!(_edit_version_infos[_apply_version_idx]->version < version)) {
            if (now - wait_start > 3000) {
                LOG(WARNING) << strings::Substitute("wait_for_version slow($0ms) version:$1 $2", now - wait_start,
                                                    version.to_string(), _debug_string(false, true));
            }
            break;
        }
        if (_edit_version_infos.back()->version < version &&
            (_pending_commits.empty() || _pending_commits.rbegin()->first < version.major())) {
            string msg = strings::Substitute("wait_for_version failed version:$0 $1", version.to_string(),
                                             _debug_string(false, true));
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        if (now - wait_start > timeout_ms) {
            string msg = strings::Substitute("wait_for_version timeout($0ms) version:$1 $2", now - wait_start,
                                             version.to_string(), _debug_string(false, true));
            LOG(WARNING) << msg;
            return Status::TimedOut(msg);
        }
    }
    return Status::OK();
}

Status TabletUpdates::_do_update(uint32_t rowset_id, int32_t upsert_idx, int32_t condition_column, int64_t read_version,
                                 const std::vector<ColumnUniquePtr>& upserts, PrimaryIndex& index, int64_t tablet_id,
                                 DeletesMap* new_deletes) {
    if (condition_column >= 0) {
        auto tablet_column = _tablet.tablet_schema().column(condition_column);
        std::vector<uint32_t> read_column_ids;
        read_column_ids.push_back(condition_column);

        std::vector<uint64_t> old_rowids(upserts[upsert_idx]->size());
        RETURN_IF_ERROR(index.get(*upserts[upsert_idx], &old_rowids));
        bool non_old_value = std::all_of(old_rowids.begin(), old_rowids.end(), [](int id) { return -1 == id; });
        if (!non_old_value) {
            std::map<uint32_t, std::vector<uint32_t>> old_rowids_by_rssid;
            size_t num_default = 0;
            vector<uint32_t> idxes;
            RowsetUpdateState::plan_read_by_rssid(old_rowids, &num_default, &old_rowids_by_rssid, &idxes);
            std::vector<std::unique_ptr<Column>> old_columns(1);
            auto old_unordered_column =
                    ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
            old_columns[0] = old_unordered_column->clone_empty();
            RETURN_IF_ERROR(get_column_values(read_column_ids, read_version, num_default > 0, old_rowids_by_rssid,
                                              &old_columns, nullptr));
            auto old_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
            old_column->append_selective(*old_columns[0], idxes.data(), 0, idxes.size());

            std::map<uint32_t, std::vector<uint32_t>> new_rowids_by_rssid;
            std::vector<uint32_t> rowids;
            for (int j = 0; j < upserts[upsert_idx]->size(); ++j) {
                rowids.push_back(j);
            }
            new_rowids_by_rssid[rowset_id + upsert_idx] = rowids;
            std::vector<std::unique_ptr<Column>> new_columns(1);
            auto new_column = ChunkHelper::column_from_field_type(tablet_column.type(), tablet_column.is_nullable());
            new_columns[0] = new_column->clone_empty();
            RETURN_IF_ERROR(get_column_values(read_column_ids, read_version, false, new_rowids_by_rssid, &new_columns,
                                              nullptr));

            int idx_begin = 0;
            int upsert_idx_step = 0;
            for (int j = 0; j < old_column->size(); ++j) {
                if (num_default > 0 && idxes[j] == 0) {
                    // plan_read_by_rssid will return idx with 0 if we have default value
                    upsert_idx_step++;
                } else {
                    int r = old_column->compare_at(j, j, *new_columns[0].get(), -1);
                    if (r > 0) {
                        RETURN_IF_ERROR(index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], idx_begin,
                                                     idx_begin + upsert_idx_step, new_deletes));

                        idx_begin = j + 1;
                        upsert_idx_step = 0;

                        // Update delete vector of current segment which is being applied
                        (*new_deletes)[rowset_id + upsert_idx].push_back(j);
                    } else {
                        upsert_idx_step++;
                    }
                }
            }

            if (idx_begin < old_column->size()) {
                RETURN_IF_ERROR(index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], idx_begin,
                                             idx_begin + upsert_idx_step, new_deletes));
            }
        } else {
            RETURN_IF_ERROR(index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], new_deletes));
        }
    } else {
        std::unique_ptr<IOStat> iostat = std::make_unique<IOStat>();
        MonotonicStopWatch watch;
        watch.start();
        RETURN_IF_ERROR(index.upsert(rowset_id + upsert_idx, 0, *upserts[upsert_idx], new_deletes, iostat.get()));
        LOG(INFO) << "primary index upsert tid: " << tablet_id << ", cost: " << watch.elapsed_time() << ", "
                  << iostat->print_str();
    }

    return Status::OK();
}

Status TabletUpdates::_do_compaction(std::unique_ptr<CompactionInfo>* pinfo) {
    int64_t input_rowsets_size = 0;
    int64_t input_row_num = 0;
    auto info = (*pinfo).get();
    vector<RowsetSharedPtr> input_rowsets(info->inputs.size());
    {
        std::lock_guard<std::mutex> lg(_rowsets_lock);
        for (size_t i = 0; i < info->inputs.size(); i++) {
            auto itr = _rowsets.find(info->inputs[i]);
            if (itr == _rowsets.end()) {
                // rowset should exists
                string msg = strings::Substitute("_do_compaction rowset $0 should exists $1", info->inputs[i],
                                                 _debug_string(false));
                LOG(ERROR) << msg;
                _set_error(msg);
                return Status::InternalError(msg);
            } else {
                input_rowsets[i] = itr->second;
                input_rowsets_size += input_rowsets[i]->data_disk_size();
                input_row_num += input_rowsets[i]->num_rows();
            }
        }
    }

    CompactionAlgorithm algorithm = CompactionUtils::choose_compaction_algorithm(
            _tablet.num_columns(), config::vertical_compaction_max_columns_per_group, input_rowsets.size());

    RowsetWriterContext context;
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = _tablet.tablet_uid();
    context.tablet_id = _tablet.tablet_id();
    context.partition_id = _tablet.partition_id();
    context.tablet_schema_hash = _tablet.schema_hash();
    context.rowset_path_prefix = _tablet.schema_hash_path();
    context.tablet_schema = &_tablet.tablet_schema();
    context.rowset_state = COMMITTED;
    context.segments_overlap = NONOVERLAPPING;
    context.max_rows_per_segment =
            CompactionUtils::get_segment_max_rows(config::max_segment_file_size, input_row_num, input_rowsets_size);
    context.writer_type =
            (algorithm == VERTICAL_COMPACTION ? RowsetWriterType::kVertical : RowsetWriterType::kHorizontal);
    std::unique_ptr<RowsetWriter> rowset_writer;
    Status st = RowsetFactory::create_rowset_writer(context, &rowset_writer);
    if (!st.ok()) {
        std::stringstream ss;
        ss << "Fail to create rowset writer err=" << st << " " << debug_string();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    MergeConfig cfg;
    cfg.chunk_size = config::vector_chunk_size;
    cfg.algorithm = algorithm;
    RETURN_IF_ERROR(
            compaction_merge_rowsets(_tablet, info->start_version.major(), input_rowsets, rowset_writer.get(), cfg));
    auto output_rowset = rowset_writer->build();
    if (!output_rowset.ok()) return output_rowset.status();
    if (config::enable_rowset_verify) {
        auto st = (*output_rowset)->verify();
        if (!st.ok()) {
            st = st.clone_and_append(
                    strings::Substitute("compaction tablet:$0 #input:$1", _tablet.tablet_id(), input_rowsets.size()));
            LOG(WARNING) << st.get_error_msg();
            return st;
        }
    }
    // 4. commit compaction
    EditVersion version;
    RETURN_IF_ERROR(_commit_compaction(pinfo, *output_rowset, &version));
    // already committed, so we can ignore timeout error here
    std::unique_lock<std::mutex> ul(_lock);
    _wait_for_version(version, 120000, ul);
    return Status::OK();
}

// We need this function to detect the conflict between column mode partial update, and compaction,
// E.g: if there are two rowsets(rowset-1, rowset-2) in tablet, current version is (2.0),
// and rowset-1 has just been partial update in column mode. and apply in version(3.0).
// But at the same time, compaction is happening, compact (rowset-1, rowset-2) into (rowset-3), but the start version is (2.0),
// at last, we will commit this compaction in version (3.1). But this compaction will miss the partial update.
// So we need `_check_conflict_with_partial_update` to detect this conflict and cancel this compaction.
Status TabletUpdates::_check_conflict_with_partial_update(CompactionInfo* info) {
    // check edit version info from latest to info->start_version
    for (auto i = _edit_version_infos.rbegin(); i != _edit_version_infos.rend() && info->start_version < (*i)->version;
         i++) {
        // check if need to cancel this compaction
        bool need_cancel = false;
        if ((*i)->deltas.size() == 0) {
            // meet full clone
            need_cancel = false;
        } else {
            uint32_t rowset_id = (*i)->deltas[0];
            RowsetSharedPtr rowset = _get_rowset(rowset_id);
            if (rowset->is_column_mode_partial_update()) {
                need_cancel = true;
            }
        }

        if (need_cancel) {
            std::string msg =
                    strings::Substitute("compaction conflict with partial update failed, tabletid:$0 ver:$1-$2",
                                        _tablet.tablet_id(), info->start_version.major(), (*i)->version.major());
            LOG(WARNING) << msg;
            _compaction_state.reset();
            return Status::Cancelled(msg);
        }
    }
    return Status::OK();
}

Status TabletUpdates::_commit_compaction(std::unique_ptr<CompactionInfo>* pinfo, const RowsetSharedPtr& rowset,
                                         EditVersion* commit_version) {
    auto span = Tracer::Instance().start_trace_tablet("commit_compaction", _tablet.tablet_id());
    auto scoped_span = trace::Scope(span);
    _compaction_state = std::make_unique<CompactionState>();
    auto status = _compaction_state->load(rowset.get());
    if (!status.ok()) {
        _compaction_state.reset();
        std::string msg = strings::Substitute("_commit_compaction error: load compaction state failed: $0 $1",
                                              status.to_string(), debug_string());
        LOG(WARNING) << msg;
        return status;
    }
    std::lock_guard wl(_lock);
    if (_edit_version_infos.empty()) {
        string msg = strings::Substitute("tablet deleted when commit_compaction tablet:$0", _tablet.tablet_id());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    EditVersionMetaPB edit;
    auto lastv = _edit_version_infos.back().get();
    // handle conflict between column mode partial update
    RETURN_IF_ERROR(_check_conflict_with_partial_update((*pinfo).get()));
    auto edit_version_pb = edit.mutable_version();
    edit_version_pb->set_major(lastv->version.major());
    edit_version_pb->set_minor(lastv->version.minor() + 1);
    int64_t creation_time = time(nullptr);
    edit.set_creation_time(creation_time);
    uint32_t rowsetid = _next_rowset_id;
    const auto& inputs = (*pinfo)->inputs;
    const auto& ors = _edit_version_infos.back()->rowsets;
    for (auto rowset_id : inputs) {
        if (std::find(ors.begin(), ors.end(), rowset_id) == ors.end()) {
            // This may happen after a full clone.
            _compaction_state.reset();
            auto msg = strings::Substitute("compaction input rowset($0) not found $1", rowset_id,
                                           _debug_string(false, false));
            LOG(WARNING) << msg;
            return Status::Cancelled(msg);
        }
    }
    CHECK(inputs.size() <= ors.size()) << strings::Substitute("compaction input size($0) > rowset size($1) tablet:$2",
                                                              inputs.size(), ors.size(), _tablet.tablet_id());
    std::vector<uint32_t> nrs = modify(ors, &rowsetid, &rowsetid + 1, inputs.begin(), inputs.end());
    if (nrs.size() <= 16) {
        // full copy
        repeated_field_add(edit.mutable_rowsets(), nrs.begin(), nrs.end());
    } else {
        // incremental
        repeated_field_add(edit.mutable_rowsets_del(), inputs.begin(), inputs.end());
        edit.add_rowsets_add(rowsetid);
    }
    // set compaction info
    auto compactionPB = edit.mutable_compaction();
    auto start_version = compactionPB->mutable_start_version();
    start_version->set_major((*pinfo)->start_version.major());
    start_version->set_minor((*pinfo)->start_version.minor());
    repeated_field_add(compactionPB->mutable_inputs(), inputs.begin(), inputs.end());
    compactionPB->add_outputs(rowsetid);

    // set rowsetid add
    uint32_t rowsetid_add = std::max(1U, (uint32_t)rowset->num_segments());
    edit.set_rowsetid_add(rowsetid_add);

    // TODO: is rollback modification of rowset meta required if commit failed?
    rowset->make_commit(edit_version_pb->major(), rowsetid);
    auto& rowset_meta = rowset->rowset_meta()->get_meta_pb();

    // TODO(cbl): impl and use TabletMetaManager::compaction commit
    auto st = TabletMetaManager::rowset_commit(_tablet.data_dir(), _tablet.tablet_id(), _next_log_id, &edit,
                                               rowset_meta, string());
    if (!st.ok()) {
        _compaction_state.reset();
        LOG(WARNING) << "compaction commit failed: " << st << " " << _debug_string(false, false);
        return st;
    }
    // apply in-memory state after commit success
    (*pinfo)->output = rowsetid;
    _next_log_id++;
    _next_rowset_id += rowsetid_add;
    std::unique_ptr<EditVersionInfo> edit_version_info = std::make_unique<EditVersionInfo>();
    edit_version_info->version = EditVersion(edit_version_pb->major(), edit_version_pb->minor());
    edit_version_info->creation_time = creation_time;
    edit_version_info->rowsets.swap(nrs);
    edit_version_info->compaction.swap(*pinfo);
    _edit_version_infos.emplace_back(std::move(edit_version_info));
    _check_creation_time_increasing();
    auto edit_version_info_ptr = _edit_version_infos.back().get();
    {
        std::lock_guard<std::mutex> lg(_rowsets_lock);
        _rowsets[rowsetid] = rowset;
    }
    {
        auto rowset_stats = std::make_unique<RowsetStats>();
        rowset_stats->num_segments = rowset->num_segments();
        rowset_stats->num_rows = rowset->num_rows();
        rowset_stats->num_dels = 0;
        rowset_stats->byte_size = rowset->data_disk_size();
        rowset_stats->partial_update_by_column = false;
        _calc_compaction_score(rowset_stats.get());

        std::lock_guard lg(_rowset_stats_lock);
        _rowset_stats.emplace(rowsetid, std::move(rowset_stats));
    }
    LOG(INFO) << "commit compaction tablet:" << _tablet.tablet_id()
              << " version:" << edit_version_info_ptr->version.to_string() << " rowset:" << rowsetid
              << " #seg:" << rowset->num_segments() << " #row:" << rowset->num_rows()
              << " size:" << PrettyPrinter::print(rowset->data_disk_size(), TUnit::BYTES)
              << " #pending:" << _pending_commits.size()
              << " state_memory:" << PrettyPrinter::print(_compaction_state->memory_usage(), TUnit::BYTES);
    VLOG(1) << "update compaction commit " << _debug_string(false, true);
    _check_for_apply();
    *commit_version = edit_version_info_ptr->version;
    span->SetAttribute("version", commit_version->to_string());
    return Status::OK();
}

void TabletUpdates::_apply_compaction_commit(const EditVersionInfo& version_info) {
    DeferOp defer([&]() { _compaction_running = false; });
    auto scoped_span = trace::Scope(Tracer::Instance().start_trace_tablet("apply_compaction", _tablet.tablet_id()));
    // NOTE: after commit, apply must success or fatal crash
    auto info = version_info.compaction.get();
    CHECK(info != nullptr) << "compaction info empty";
    // if compaction_state == null, it must be the case that BE restarted
    // need to rebuild/load state from disk
    if (!_compaction_state) {
        _compaction_state = std::make_unique<CompactionState>();
    }
    int64_t t_start = MonotonicMillis();
    auto manager = StorageEngine::instance()->update_manager();
    auto tablet_id = _tablet.tablet_id();
    uint32_t rowset_id = version_info.compaction->output;
    auto& version = version_info.version;
    LOG(INFO) << "apply_compaction_commit start tablet:" << tablet_id << " version:" << version_info.version.to_string()
              << " rowset:" << rowset_id;
    // 1. load index
    std::lock_guard lg(_index_lock);
    auto index_entry = manager->index_cache().get_or_create(tablet_id);
    index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    auto& index = index_entry->value();
    auto st = index.load(&_tablet);
    // `enable_persistent_index` of tablet maybe change by alter, we should get `enable_persistent_index` from index to
    // avoid inconsistency between persistent index file and PersistentIndexMeta
    bool enable_persistent_index = index.enable_persistent_index();
    if (!st.ok()) {
        manager->index_cache().remove(index_entry);
        _compaction_state.reset();
        std::string msg = strings::Substitute("_apply_compaction_commit error: load primary index failed: $0 $1",
                                              st.to_string(), debug_string());
        LOG(ERROR) << msg;
        _set_error(msg);
        return;
    }
    Rowset* output_rowset = _get_rowset(rowset_id).get();
    if (output_rowset == nullptr) {
        string msg = strings::Substitute("_apply_compaction_commit rowset not found tablet=$0 rowset=$1",
                                         _tablet.tablet_id(), rowset_id);
        LOG(ERROR) << msg;
        _set_error(msg);
        return;
    }
    if (!(st = _compaction_state->load(output_rowset)).ok()) {
        manager->index_cache().release(index_entry);
        _compaction_state.reset();
        std::string msg = strings::Substitute("_apply_compaction_commit error: load compaction state failed: $0 $1",
                                              st.to_string(), debug_string());
        LOG(ERROR) << msg;
        _set_error(msg);
        return;
    }
    index.prepare(version, 0);
    int64_t t_load = MonotonicMillis();
    // 2. iterator new rowset's pks, update primary index, generate delvec
    size_t total_deletes = 0;
    size_t total_rows = 0;
    vector<std::pair<uint32_t, DelVectorPtr>> delvecs;
    vector<uint32_t> tmp_deletes;

    // Since value stored in info->inputs of CompactInfo is rowset id
    // we should get the real max rssid here by segment number
    uint32_t max_rowset_id = *std::max_element(info->inputs.begin(), info->inputs.end());
    Rowset* rowset = _get_rowset(max_rowset_id).get();
    if (rowset == nullptr) {
        string msg = strings::Substitute("_apply_compaction_commit rowset not found tablet=$0 rowset=$1",
                                         _tablet.tablet_id(), max_rowset_id);
        LOG(ERROR) << msg;
        _set_error(msg);
        return;
    }
    uint32_t max_src_rssid = max_rowset_id + rowset->num_segments() - 1;

    for (size_t i = 0; i < _compaction_state->pk_cols.size(); i++) {
        if (st = _compaction_state->load_segments(output_rowset, i); !st.ok()) {
            manager->index_cache().release(index_entry);
            _compaction_state.reset();
            std::string msg = strings::Substitute("_apply_compaction_commit error: load compaction state failed: $0 $1",
                                                  st.to_string(), debug_string());
            LOG(ERROR) << msg;
            _set_error(msg);
            return;
        }
        auto& pk_col = _compaction_state->pk_cols[i];
        total_rows += pk_col->size();
        uint32_t rssid = rowset_id + i;
        tmp_deletes.clear();
        // replace will not grow hashtable, so don't need to check memory limit
        index.try_replace(rssid, 0, *pk_col, max_src_rssid, &tmp_deletes);
        DelVectorPtr dv = std::make_shared<DelVector>();
        if (tmp_deletes.empty()) {
            dv->init(version.major(), nullptr, 0);
        } else {
            dv->init(version.major(), tmp_deletes.data(), tmp_deletes.size());
            total_deletes += tmp_deletes.size();
        }
        delvecs.emplace_back(rssid, dv);
        _compaction_state->release_segments(rowset, i);
    }
    // release memory
    _compaction_state.reset();
    int64_t t_index_delvec = MonotonicMillis();

    PersistentIndexMetaPB index_meta;
    if (enable_persistent_index) {
        st = TabletMetaManager::get_persistent_index_meta(_tablet.data_dir(), tablet_id, &index_meta);
        if (!st.ok() && !st.is_not_found()) {
            std::string msg = strings::Substitute("get persistent index meta failed: $0", st.to_string());
            LOG(ERROR) << msg << " " << _debug_string(false, true);
            _set_error(msg);
            return;
        }
    }
    st = index.commit(&index_meta);
    if (!st.ok()) {
        std::string msg = strings::Substitute("primary index commit failed: $0", st.to_string());
        LOG(ERROR) << msg << " " << _debug_string(false, true);
        _set_error(msg);
        return;
    }

    {
        std::lock_guard wl(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet deleted when apply compaction tablet:" << tablet_id;
            return;
        }
        // 3. write meta
        st = TabletMetaManager::apply_rowset_commit(_tablet.data_dir(), tablet_id, _next_log_id, version_info.version,
                                                    delvecs, index_meta, enable_persistent_index, nullptr);
        if (!st.ok()) {
            manager->index_cache().release(index_entry);
            std::string msg = strings::Substitute("_apply_compaction_commit error: write meta failed: $0 $1",
                                                  st.to_string(), _debug_string(false));
            LOG(ERROR) << msg;
            _set_error(msg);
            return;
        }
        // 4. put delvec in cache
        TabletSegmentId tsid;
        tsid.tablet_id = _tablet.tablet_id();
        for (auto& delvec_pair : delvecs) {
            tsid.segment_id = delvec_pair.first;
            manager->set_cached_del_vec(tsid, delvec_pair.second);
            // try to set empty dcg cache, for improving latency when reading
            manager->set_cached_empty_delta_column_group(_tablet.data_dir()->get_meta(), tsid);
        }
        // 5. apply memory
        _next_log_id++;
        _apply_version_idx++;
        _apply_version_changed.notify_all();
    }

    st = index.on_commited();
    if (!st.ok()) {
        std::string msg = strings::Substitute("primary index on_commit failed: $0", st.to_string());
        LOG(ERROR) << msg;
        _set_error(msg);
        return;
    }

    // if `enable_persistent_index` of tablet is change(maybe changed by alter table)
    // we should try to remove the index_entry from cache
    // Otherwise index may be used for later commits, keep in cache
    if (enable_persistent_index ^ _tablet.get_enable_persistent_index()) {
        manager->index_cache().remove(index_entry);
    } else {
        manager->index_cache().release(index_entry);
    }

    {
        // Update the stats of affected rowsets.
        std::lock_guard lg(_rowset_stats_lock);
        auto iter = _rowset_stats.find(rowset_id);
        if (iter == _rowset_stats.end()) {
            string msg = strings::Substitute("inconsistent rowset_stats, rowset not found tablet=$0 rowsetid=$1",
                                             _tablet.tablet_id(), rowset_id);
            DCHECK(false) << msg;
            LOG(ERROR) << msg;
        } else {
            DCHECK_EQ(iter->second->num_dels, 0);
            iter->second->num_dels += total_deletes;
            _calc_compaction_score(iter->second.get());
            DCHECK_EQ(iter->second->num_dels, _get_rowset_num_deletes(rowset_id));
            DCHECK_EQ(iter->second->num_rows, total_rows);
            DCHECK_LE(iter->second->num_dels, iter->second->num_rows);
        }
    }
    size_t row_before = 0;
    size_t row_after = 0;
    _update_total_stats(version_info.rowsets, &row_before, &row_after);
    int64_t t_write = MonotonicMillis();
    size_t del_percent = _cur_total_rows == 0 ? 0 : (_cur_total_dels * 100) / _cur_total_rows;
    LOG(INFO) << "apply_compaction_commit finish tablet:" << tablet_id
              << " version:" << version_info.version.to_string() << " total del/row:" << _cur_total_dels << "/"
              << _cur_total_rows << " " << del_percent << "%"
              << " rowset:" << rowset_id << " #row:" << total_rows << " #del:" << total_deletes
              << " #delvec:" << delvecs.size() << " duration:" << t_write - t_start << "ms"
              << strings::Substitute("($0/$1/$2)", t_load - t_start, t_index_delvec - t_load, t_write - t_index_delvec);
    VLOG(1) << "update compaction apply " << _debug_string(true, true);
    if (row_before != row_after) {
        string msg = strings::Substitute("actual row size changed after compaction $0 -> $1", row_before, row_after);
        LOG(ERROR) << msg << debug_string();
        _set_error(msg + _debug_version_info(true));
    }
}

void TabletUpdates::to_updates_pb(TabletUpdatesPB* updates_pb) const {
    std::lock_guard rl(_lock);
    _to_updates_pb_unlocked(updates_pb);
}

bool TabletUpdates::check_rowset_id(const RowsetId& rowset_id) const {
    // TODO(cbl): optimization: check multiple rowset_ids at once
    {
        std::lock_guard l(_rowsets_lock);
        for (const auto& [id, rowset] : _rowsets) {
            if (rowset->rowset_id() == rowset_id) {
                return true;
            }
        }
    }
    {
        std::lock_guard rl(_lock);
        for (auto& pending_commit : _pending_commits) {
            if (pending_commit.second->rowset_id() == rowset_id) {
                return true;
            }
        }
    }
    return false;
}

void TabletUpdates::remove_expired_versions(int64_t expire_time) {
    if (_error) {
        LOG(WARNING) << strings::Substitute(
                "remove_expired_versions failed, tablet updates is in error state: tablet:$0 $1", _tablet.tablet_id(),
                _error_msg);
        return;
    }

    size_t num_version_removed = 0;
    size_t num_rowset_removed = 0;
    uint64_t min_readable_version = 0;
    // GC works that require locking
    {
        std::lock_guard l(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet deleted when erase_expired_versions tablet:" << _tablet.tablet_id();
            return;
        }

        // only keep at most one version which is before expire_time
        // also to prevent excessive memory usage of editversion array, limit edit version count to be less than
        // config::tablet_max_versions
        size_t keep_index_min = _apply_version_idx;
        while (keep_index_min > 0) {
            if (_edit_version_infos[keep_index_min]->creation_time <= expire_time ||
                keep_index_min + config::tablet_max_versions < _edit_version_infos.size()) {
                break;
            }
            keep_index_min--;
        }
        num_version_removed = keep_index_min;
        if (num_version_removed > 0) {
            _edit_version_infos.erase(_edit_version_infos.begin(), _edit_version_infos.begin() + num_version_removed);
            _apply_version_idx -= num_version_removed;
            // remove non-referenced rowsets
            std::set<uint32_t> active_rowsets;
            for (const auto& edit_version_info : _edit_version_infos) {
                active_rowsets.insert(edit_version_info->rowsets.begin(), edit_version_info->rowsets.end());
            }
            std::lock_guard rl(_rowsets_lock);
            std::lock_guard rsl(_rowset_stats_lock);
            for (auto it = _rowsets.begin(); it != _rowsets.end();) {
                if (active_rowsets.find(it->first) == active_rowsets.end()) {
                    num_rowset_removed++;
                    _rowset_stats.erase(it->first);
                    (void)_unused_rowsets.blocking_put(std::move(it->second));
                    it = _rowsets.erase(it);
                } else {
                    ++it;
                }
            }
            min_readable_version = _edit_version_infos[0]->version.major();
        }
    }

    // GC works that can be done outside of lock
    if (num_version_removed > 0) {
        {
            std::unique_lock wrlock(_tablet.get_header_lock());
            _tablet.save_meta();
        }
        auto tablet_id = _tablet.tablet_id();
        // Remove useless delete vectors.
        auto meta_store = _tablet.data_dir()->get_meta();
        auto res = TabletMetaManager::delete_del_vector_before_version(meta_store, tablet_id, min_readable_version);
        size_t delvec_deleted = 0;
        if (!res.ok()) {
            LOG(WARNING) << "Fail to delete_del_vector_before_version tablet:" << tablet_id
                         << " min_readable_version:" << min_readable_version << " msg:" << res.status();
        } else {
            delvec_deleted = res.value();
        }
        // Remove useless delta column group
        auto update_manager = StorageEngine::instance()->update_manager();
        size_t dcg_deleted = 0;
        res = update_manager->clear_delta_column_group_before_version(meta_store, tablet_id, min_readable_version);
        if (!res.ok()) {
            LOG(WARNING) << "Fail to clear_delta_column_group_before_version tablet:" << tablet_id
                         << " min_readable_version:" << min_readable_version << " msg:" << res.status();
        } else {
            dcg_deleted = res.value();
        }
        LOG(INFO) << strings::Substitute(
                "remove_expired_versions $0 time:$1 min_readable_version:$2 deletes: #version:$3 #rowset:$4 "
                "#delvec:$5 #dcgs:$6",
                _debug_version_info(true), expire_time, min_readable_version, num_version_removed, num_rowset_removed,
                delvec_deleted, dcg_deleted);
    }
    _remove_unused_rowsets();
}

int64_t TabletUpdates::get_compaction_score() {
    if (_compaction_running || _error) {
        // don't do compaction
        return -1;
    }
    if (_last_compaction_time_ms + config::update_compaction_per_tablet_min_interval_seconds * 1000 > UnixMillis()) {
        // don't do compaction
        return -1;
    }
    vector<uint32_t> rowsets;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            return -1;
        }
        if (_apply_version_idx + 2 < _edit_version_infos.size() || _pending_commits.size() >= 2) {
            // has too many pending tasks, skip compaction
            size_t version_count = _edit_version_infos.back()->rowsets.size() + _pending_commits.size();
            if (version_count > config::tablet_max_versions) {
                LOG(INFO) << strings::Substitute(
                        "Try to do compaction because of too many versions. tablet_id:$0 "
                        "version_count:$1 limit:$2 applied_version_idx:$3 edit_version_infos:$4 pending:$5",
                        _tablet.tablet_id(), version_count, config::tablet_max_versions, _apply_version_idx,
                        _edit_version_infos.size(), _pending_commits.size());
            } else {
                return -1;
            }
        }
        for (size_t i = _apply_version_idx + 1; i < _edit_version_infos.size(); i++) {
            if (_edit_version_infos[i]->compaction) {
                // has pending compaction not finished, do not do compaction
                return -1;
            }
        }
        rowsets = _edit_version_infos[_apply_version_idx]->rowsets;
    }
    int64_t total_score = 0;
    size_t total_inputs = 0;
    size_t total_deletes = 0;
    bool has_error = false;
    {
        std::lock_guard lg(_rowset_stats_lock);
        for (auto rowsetid : rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr == _rowset_stats.end()) {
                // should not happen
                string msg = strings::Substitute("rowset not found in rowset stats tablet=$0 rowset=$1",
                                                 _tablet.tablet_id(), rowsetid);
                DCHECK(false) << msg;
                LOG(WARNING) << msg;
                has_error = true;
                break;
            } else if (itr->second->compaction_score > 0) {
                total_score += itr->second->compaction_score;
                total_inputs += std::max(1UL, itr->second->num_segments);
                total_deletes += itr->second->num_dels;
            }
        }
    }
    if (has_error) {
        LOG(WARNING) << "error get_compaction_score: " << debug_string();
        // do not do compaction
        return -1;
    }
    if (total_inputs == 1 && total_deletes == 0) {
        // only 1 input and no delete, no need to do compaction
        return -1;
    }
    return total_score;
}

struct CompactionEntry {
    float score_per_row = 0.0f;
    uint32_t rowsetid = 0;
    size_t num_rows = 0;
    size_t num_dels = 0;
    size_t bytes = 0;
    size_t num_segments = 0;

    bool operator<(const CompactionEntry& rhs) const { return score_per_row > rhs.score_per_row; }
};

static string int_list_to_string(const vector<uint32_t>& l) {
    string ret;
    for (size_t i = 0; i < l.size(); i++) {
        if (i > 0) {
            ret.append(",");
        }
        ret.append(std::to_string(l[i]));
    }
    return ret;
}

Status TabletUpdates::compaction(MemTracker* mem_tracker) {
    if (_error) {
        return Status::InternalError(strings::Substitute(
                "compaction failed, tablet updates is in error state: tablet:$0 $1", _tablet.tablet_id(), _error_msg));
    }
    bool was_runing = false;
    if (!_compaction_running.compare_exchange_strong(was_runing, true)) {
        return Status::InternalError("illegal state: another compaction is running");
    }
    std::unique_ptr<CompactionInfo> info = std::make_unique<CompactionInfo>();
    vector<uint32_t> rowsets;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            string msg = strings::Substitute("tablet deleted when compaction tablet:$0", _tablet.tablet_id());
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        // 1. start compaction at current apply version
        info->start_version = _edit_version_infos[_apply_version_idx]->version;
        rowsets = _edit_version_infos[_apply_version_idx]->rowsets;
    }
    size_t total_valid_rowsets = 0;
    size_t total_rows = 0;
    size_t total_bytes = 0;
    size_t total_segments = 0;
    size_t total_rows_after_compaction = 0;
    size_t total_bytes_after_compaction = 0;
    bool has_partial_update_by_column = false;
    int64_t total_score = 0;
    vector<CompactionEntry> candidates;
    {
        std::lock_guard lg(_rowset_stats_lock);
        for (auto rowsetid : rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr == _rowset_stats.end()) {
                // should not happen
                string msg = strings::Substitute("rowset not found in rowset stats tablet=$0 rowset=$1",
                                                 _tablet.tablet_id(), rowsetid);
                DCHECK(false) << msg;
                LOG(WARNING) << msg;
            } else if (itr->second->compaction_score > 0) {
                auto& stat = *itr->second;
                total_valid_rowsets++;
                if (stat.num_rows == stat.num_dels) {
                    // add to compaction directly
                    info->inputs.push_back(itr->first);
                    total_score += stat.compaction_score;
                    total_rows += stat.num_rows;
                    total_bytes += stat.byte_size;
                    total_segments += stat.num_segments;
                    // rowset with partial update by column, should contains zero rows and dels.
                    has_partial_update_by_column |= stat.partial_update_by_column;
                    continue;
                }
                candidates.emplace_back();
                auto& e = candidates.back();
                e.rowsetid = itr->first;
                e.score_per_row = (float)((double)stat.compaction_score / (stat.num_rows - stat.num_dels));
                e.num_rows = stat.num_rows;
                e.num_dels = stat.num_dels;
                e.bytes = stat.byte_size;
                e.num_segments = stat.num_segments;
            }
        }
    }
    std::sort(candidates.begin(), candidates.end());
    for (auto& e : candidates) {
        size_t new_rows = total_rows_after_compaction + e.num_rows - e.num_dels;
        size_t new_bytes = total_bytes_after_compaction + e.bytes * (e.num_rows - e.num_dels) / e.num_rows;
        if (info->inputs.size() > 0 && new_bytes > config::update_compaction_result_bytes * 2) {
            break;
        }
        // Partial update generate empty rowset, compact them first.
        // Or partial update by column will trigger too many useless compaction cost.
        if (info->inputs.size() > 1 && has_partial_update_by_column) {
            break;
        }
        info->inputs.push_back(e.rowsetid);
        total_score += e.score_per_row * (e.num_rows - e.num_dels);
        total_rows += e.num_rows;
        total_bytes += e.bytes;
        total_segments += e.num_segments;
        total_rows_after_compaction = new_rows;
        total_bytes_after_compaction = new_bytes;
        if (total_bytes_after_compaction > config::update_compaction_result_bytes ||
            info->inputs.size() >= config::max_update_compaction_num_singleton_deltas) {
            break;
        }
    }
    if (total_valid_rowsets - info->inputs.size() <= 3) {
        // give 10s time gitter, so same table's compaction don't start at same time
        _last_compaction_time_ms = UnixMillis() + rand() % 10000;
    }
    if (info->inputs.empty()) {
        LOG(INFO) << "no candidate rowset to do update compaction, tablet:" << _tablet.tablet_id();
        _compaction_running = false;
        return Status::OK();
    }
    std::sort(info->inputs.begin(), info->inputs.end());
    // else there are still many(>3) rowset's need's to be compacted,
    // do not reset _last_compaction_time_ms so we can continue doing compaction
    LOG(INFO) << "update compaction start tablet:" << _tablet.tablet_id()
              << " version:" << info->start_version.to_string() << " score:" << total_score
              << " pick:" << info->inputs.size() << "/valid:" << total_valid_rowsets << "/all:" << rowsets.size() << " "
              << int_list_to_string(info->inputs) << " #segments:" << total_segments << " #rows:" << total_rows << "->"
              << total_rows_after_compaction << " bytes:" << PrettyPrinter::print(total_bytes, TUnit::BYTES) << "->"
              << PrettyPrinter::print(total_bytes_after_compaction, TUnit::BYTES) << "(estimate)";

    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(mem_tracker);
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    Status st = _do_compaction(&info);
    if (!st.ok()) {
        _compaction_running = false;
        _last_compaction_failure_millis = UnixMillis();
    } else {
        _last_compaction_success_millis = UnixMillis();
    }
    return st;
}

Status TabletUpdates::get_rowsets_for_compaction(int64_t rowset_size_threshold, std::vector<uint32_t>& rowset_ids,
                                                 size_t& total_bytes) {
    std::vector<uint32_t> all_rowsets;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            string msg = strings::Substitute("tablet deleted when get_rowsets_for_compaction tablet:$0",
                                             _tablet.tablet_id());
            LOG(WARNING) << msg;
            _compaction_running = false;
            return Status::InternalError(msg);
        }
        all_rowsets = _edit_version_infos[_apply_version_idx]->rowsets;
    }
    total_bytes = 0;
    {
        std::lock_guard lg(_rowset_stats_lock);
        for (auto rowsetid : all_rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr == _rowset_stats.end()) {
                // should not happen
                string msg = strings::Substitute("rowset not found in rowset stats tablet=$0 rowset=$1",
                                                 _tablet.tablet_id(), rowsetid);
                DCHECK(false) << msg;
                LOG(WARNING) << msg;
                continue;
            }
            auto& stat = *itr->second;
            if (stat.byte_size < rowset_size_threshold) {
                rowset_ids.push_back(rowsetid);
                total_bytes += stat.byte_size;
            }
        }
    }
    return Status::OK();
}

Status TabletUpdates::compaction(MemTracker* mem_tracker, const vector<uint32_t>& input_rowset_ids) {
    if (_error) {
        return Status::InternalError(strings::Substitute(
                "compaction failed, tablet updates is in error state: tablet:$0 $1", _tablet.tablet_id(), _error_msg));
    }
    bool was_runing = false;
    if (!_compaction_running.compare_exchange_strong(was_runing, true)) {
        return Status::InternalError("illegal state: another compaction is running");
    }
    std::unique_ptr<CompactionInfo> info = std::make_unique<CompactionInfo>();
    std::unordered_set<uint32_t> all_rowsets;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            string msg = strings::Substitute("tablet deleted when compaction tablet:$0", _tablet.tablet_id());
            LOG(WARNING) << msg;
            _compaction_running = false;
            return Status::InternalError(msg);
        }
        // 1. start compaction at current apply version
        info->start_version = _edit_version_infos[_apply_version_idx]->version;
        const auto& rowsets = _edit_version_infos[_apply_version_idx]->rowsets;
        all_rowsets.insert(rowsets.begin(), rowsets.end());
    }
    size_t total_rows = 0;
    size_t total_bytes = 0;
    size_t total_rows_after_compaction = 0;
    size_t total_bytes_after_compaction = 0;
    size_t total_segments = 0;
    {
        std::lock_guard lg(_rowset_stats_lock);
        for (auto rowsetid : input_rowset_ids) {
            if (all_rowsets.find(rowsetid) == all_rowsets.end()) {
                LOG(WARNING) << "specified input rowset not found in current version rowsetid:" << rowsetid;
                continue;
            }
            auto itr = _rowset_stats.find(rowsetid);
            if (itr == _rowset_stats.end()) {
                // should not happen
                string msg = strings::Substitute("rowset not found in rowset stats tablet=$0 rowset=$1",
                                                 _tablet.tablet_id(), rowsetid);
                DCHECK(false) << msg;
                LOG(WARNING) << msg;
                continue;
            }
            auto& stat = *itr->second;
            info->inputs.push_back(itr->first);
            if (stat.num_rows > 0) {
                total_rows += stat.num_rows;
                total_bytes += stat.byte_size;
                total_rows_after_compaction += stat.num_rows - stat.num_dels;
                total_bytes_after_compaction += stat.byte_size * (stat.num_rows - stat.num_dels) / stat.num_rows;
            }
            total_segments += stat.num_segments;
        }
    }
    if (info->inputs.empty()) {
        LOG(INFO) << "no candidate rowset to do update compaction, tablet:" << _tablet.tablet_id();
        _compaction_running = false;
        return Status::OK();
    }
    if (total_segments == 1) {
        LOG(INFO) << "only 1 segment, skip update compaction, tablet:" << _tablet.tablet_id();
        _compaction_running = false;
        return Status::OK();
    }
    // do not reset _last_compaction_time_ms so we can continue doing compaction
    std::sort(info->inputs.begin(), info->inputs.end());
    LOG(INFO) << "update compaction with specified rowsets start tablet:" << _tablet.tablet_id()
              << " version:" << info->start_version.to_string() << " pick:" << info->inputs.size()
              << "/all:" << all_rowsets.size() << " " << int_list_to_string(info->inputs) << " #rows:" << total_rows
              << "->" << total_rows_after_compaction << " bytes:" << PrettyPrinter::print(total_bytes, TUnit::BYTES)
              << "->" << PrettyPrinter::print(total_bytes_after_compaction, TUnit::BYTES) << "(estimate)";

    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(mem_tracker);
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    Status st = _do_compaction(&info);
    if (!st.ok()) {
        _compaction_running = false;
        _last_compaction_failure_millis = UnixMillis();
    } else {
        _last_compaction_success_millis = UnixMillis();
    }
    return st;
}

StatusOr<std::vector<std::pair<uint32_t, uint32_t>>> TabletUpdates::list_rowsets_need_repair_compaction() {
    if (_error) {
        return Status::InternalError(strings::Substitute(
                "list_old_rowsets_with_small_segment_files failed, tablet updates is in error state: tablet:$0 $1",
                _tablet.tablet_id(), _error_msg));
    }
    vector<uint32_t> rowsets;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            string msg = strings::Substitute("tablet deleted when compaction tablet:$0", _tablet.tablet_id());
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        rowsets = _edit_version_infos[_apply_version_idx]->rowsets;
    }
    std::vector<std::pair<uint32_t, uint32_t>> ret;
    {
        std::lock_guard lg(_rowsets_lock);
        for (auto rowsetid : rowsets) {
            auto itr = _rowsets.find(rowsetid);
            if (itr == _rowsets.end()) {
                string msg = strings::Substitute("rowset not found tablet=$0 rowset=$1", _tablet.tablet_id(), rowsetid);
                DCHECK(false) << msg;
                LOG(WARNING) << msg;
                continue;
            }
            size_t bytes = itr->second->data_disk_size();
            size_t num_segments = itr->second->num_segments();
            // average segment file size < 1M and num of segment file > 10
            if (num_segments > 10 && bytes / num_segments < 1024 * 1024) {
                ret.emplace_back(rowsetid, num_segments);
            }
        }
    }
    return ret;
}

void TabletUpdates::get_compaction_status(std::string* json_result) {
    rapidjson::Document root;
    root.SetObject();

    EditVersion last_version;
    std::vector<RowsetSharedPtr> rowsets;
    std::vector<uint32_t> rowset_ids;
    bool compaction_running = _compaction_running.load();
    std::vector<RowsetSharedPtr> apply_version_rowsets;
    std::vector<uint32_t> apply_version_rowset_ids;
    {
        std::lock_guard l1(_lock);
        if (_edit_version_infos.empty()) {
            return;
        }
        std::lock_guard l2(_rowsets_lock);
        last_version = _edit_version_infos.back()->version;
        rowset_ids = _edit_version_infos.back()->rowsets;
        std::sort(rowset_ids.begin(), rowset_ids.end());
        rowsets.reserve(rowset_ids.size());
        for (unsigned int& rowset_id : rowset_ids) {
            auto it = _rowsets.find(rowset_id);
            if (it != _rowsets.end()) {
                rowsets.push_back(it->second);
            }
        }

        apply_version_rowset_ids = _edit_version_infos[_apply_version_idx]->rowsets;
        std::sort(apply_version_rowset_ids.begin(), apply_version_rowset_ids.end());
        apply_version_rowsets.reserve(apply_version_rowset_ids.size());
        for (unsigned int& rowset_id : apply_version_rowset_ids) {
            auto it = _rowsets.find(rowset_id);
            if (it != _rowsets.end()) {
                apply_version_rowsets.push_back(it->second);
            }
        }
    }

    rapidjson::Value compaction_status;
    std::string compaction_status_value = compaction_running ? "RUNNING" : "NO_RUNNING_TASK";
    compaction_status.SetString(compaction_status_value.c_str(), compaction_status_value.length(), root.GetAllocator());
    root.AddMember("compaction_status", compaction_status, root.GetAllocator());

    rapidjson::Value last_compaction_success_time;
    std::string format_str = ToStringFromUnixMillis(_last_compaction_success_millis.load());
    last_compaction_success_time.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last compaction success time", last_compaction_success_time, root.GetAllocator());

    rapidjson::Value last_compaction_failure_time;
    format_str = ToStringFromUnixMillis(_last_compaction_failure_millis.load());
    last_compaction_failure_time.SetString(format_str.c_str(), format_str.length(), root.GetAllocator());
    root.AddMember("last compaction failure time", last_compaction_failure_time, root.GetAllocator());

    rapidjson::Value rowsets_count;
    rowsets_count.SetUint64(rowsets.size());
    root.AddMember("rowsets_count", rowsets_count, root.GetAllocator());

    rapidjson::Value last_version_value;
    std::string last_version_str = strings::Substitute("$0_$1", last_version.major(), last_version.minor());
    rowsets_count.SetString(last_version_str.c_str(), last_version_str.size(), root.GetAllocator());
    root.AddMember("last_version", last_version_value, root.GetAllocator());

    rapidjson::Document rowset_details;
    rowset_details.SetArray();
    for (int i = 0; i < rowset_ids.size(); ++i) {
        rapidjson::Value value;
        value.SetObject();

        rapidjson::Value rowset_id;
        std::string rowset_id_value = rowsets[i]->rowset_id().to_string();
        rowset_id.SetString(rowset_id_value.c_str(), rowset_id_value.length(), root.GetAllocator());
        value.AddMember("rowset_id", rowset_id, root.GetAllocator());

        rapidjson::Value num_segments;
        num_segments.SetInt64(rowsets[i]->num_segments());
        value.AddMember("num_segments", num_segments, root.GetAllocator());

        rapidjson::Value rowset_size;
        rowset_size.SetInt64(rowsets[i]->data_disk_size());
        value.AddMember("rowset_size", rowset_size, root.GetAllocator());

        rowset_details.PushBack(value, rowset_details.GetAllocator());
    }
    root.AddMember("rowset_details", rowset_details, root.GetAllocator());

    rapidjson::Document apply_rowset_details;
    apply_rowset_details.SetArray();
    for (int i = 0; i < apply_version_rowset_ids.size(); ++i) {
        rapidjson::Value value;
        value.SetObject();

        rapidjson::Value rowset_id;
        std::string rowset_id_value = rowsets[i]->rowset_id().to_string();
        rowset_id.SetString(rowset_id_value.c_str(), rowset_id_value.length(), root.GetAllocator());
        value.AddMember("rowset_id", rowset_id, root.GetAllocator());

        rapidjson::Value num_segments;
        num_segments.SetInt64(rowsets[i]->num_segments());
        value.AddMember("num_segments", num_segments, root.GetAllocator());

        rapidjson::Value rowset_size;
        rowset_size.SetInt64(rowsets[i]->data_disk_size());
        value.AddMember("rowset_size", rowset_size, root.GetAllocator());

        apply_rowset_details.PushBack(value, apply_rowset_details.GetAllocator());
    }
    root.AddMember("apply_rowset_details", apply_rowset_details, root.GetAllocator());

    // to json string
    rapidjson::StringBuffer strbuf;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(strbuf);
    root.Accept(writer);
    *json_result = std::string(strbuf.GetString());
}

void TabletUpdates::_calc_compaction_score(RowsetStats* stats) {
    if (stats->num_rows == 0) {
        stats->compaction_score = config::update_compaction_size_threshold;
        return;
    }
    // TODO(cbl): estimate read/write cost, currently just use fixed value
    const int64_t cost_record_write = 1;
    const int64_t cost_record_read = 4;
    // use double to prevent overflow
    auto delete_bytes = (int64_t)(stats->byte_size * (double)stats->num_dels / stats->num_rows);
    stats->compaction_score =
            config::update_compaction_size_threshold * (stats->num_segments > 1 ? stats->num_segments - 1 : 1) +
            (cost_record_read + cost_record_write) * delete_bytes - cost_record_write * stats->byte_size;
}

size_t TabletUpdates::_get_rowset_num_deletes(uint32_t rowsetid) {
    auto rowset = _get_rowset(rowsetid);
    return (rowset == nullptr) ? 0 : _get_rowset_num_deletes(*rowset);
}

size_t TabletUpdates::_get_rowset_num_deletes(const Rowset& rowset) {
    size_t num_dels = 0;
    auto rowsetid = rowset.rowset_meta()->get_rowset_seg_id();
    for (int i = 0; i < rowset.num_segments(); i++) {
        int64_t dummy;
        DelVector delvec;
        auto st = TabletMetaManager::get_del_vector(_tablet.data_dir()->get_meta(), _tablet.tablet_id(), rowsetid + i,
                                                    INT64_MAX, &delvec, &dummy);
        if (!st.ok()) {
            LOG(WARNING) << "_refresh_rowset_stats: error get del vector " << st;
            continue;
        }
        num_dels += delvec.cardinality();
    }
    return num_dels;
}

void TabletUpdates::get_tablet_info_extra(TTabletInfo* info) {
    int64_t min_readable_version = 0;
    int64_t version = 0;
    bool has_pending = false;
    int64_t version_count = 0;
    vector<uint32_t> rowsets;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet delete when get_tablet_info_extra tablet:" << _tablet.tablet_id();
        } else {
            min_readable_version = _edit_version_infos[0]->version.major();
            auto& last = _edit_version_infos.back();
            version = last->version.major();
            rowsets = last->rowsets;
            has_pending = _pending_commits.size() > 0;
            // version count in primary key table contains two parts:
            // 1. rowsets in newest version. 2. pending rowsets.
            version_count = rowsets.size() + _pending_commits.size();
        }
    }
    string err_rowsets;
    int64_t total_row = 0;
    int64_t total_size = 0;
    {
        std::lock_guard lg(_rowset_stats_lock);
        for (uint32_t rowsetid : rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr != _rowset_stats.end()) {
                // TODO(cbl): also report num deletes
                total_row += itr->second->num_rows;
                total_size += itr->second->byte_size;
            } else {
                StringAppendF(&err_rowsets, "%u,", rowsetid);
            }
        }
    }
    if (!err_rowsets.empty()) {
        LOG_EVERY_N(WARNING, 10) << "get_tablet_info_extra() some rowset stats not found tablet=" << _tablet.tablet_id()
                                 << " rowset=" << err_rowsets;
    }
    info->__set_version(version);
    info->__set_min_readable_version(min_readable_version);
    info->__set_version_miss(has_pending);
    info->__set_version_count(version_count);
    info->__set_row_count(total_row);
    info->__set_data_size(total_size);
    info->__set_is_error_state(_error);
}

int64_t TabletUpdates::get_average_row_size() {
    int64_t row_num = 0;
    int64_t total_row_size = 0;
    vector<uint32_t> rowsets;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet delete when get_tablet_info_extra tablet:" << _tablet.tablet_id();
        } else {
            auto& last = _edit_version_infos.back();
            rowsets = last->rowsets;
        }
    }
    {
        std::lock_guard lg(_rowset_stats_lock);
        for (uint32_t rowsetid : rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr != _rowset_stats.end()) {
                // TODO(cbl): also report num deletes
                row_num += itr->second->num_rows;
                total_row_size += itr->second->row_size;
            }
        }
    }

    if (row_num != 0) {
        return total_row_size / row_num;
    } else {
        return 0;
    }
}

std::string TabletUpdates::RowsetStats::to_string() const {
    return strings::Substitute("[seg:$0 row:$1 del:$2 bytes:$3 compaction:$4 partial_update_by_column:$5]",
                               num_segments, num_rows, num_dels, byte_size, compaction_score, partial_update_by_column);
}

std::string TabletUpdates::debug_string() const {
    return _debug_string(true);
}

std::string TabletUpdates::_debug_string(bool lock, bool abbr) const {
    size_t num_version;
    size_t apply_idx;
    EditVersion first_version;
    EditVersion apply_version;
    EditVersion last_version;
    vector<uint32_t> rowsets;
    string pending_info;
    if (lock) _lock.lock();
    num_version = _edit_version_infos.size();
    // num_version can be 0, if clear_meta is called after deleting this Tablet
    if (num_version == 0) {
        if (lock) _lock.unlock();
        return strings::Substitute("tablet:$0 <deleted>", _tablet.tablet_id());
    }
    apply_idx = _apply_version_idx;
    first_version = _edit_version_infos[0]->version;
    apply_version = _edit_version_infos[_apply_version_idx]->version;
    last_version = _edit_version_infos.back()->version;
    rowsets = _edit_version_infos.back()->rowsets;
    for (auto const& pending_commit : _pending_commits) {
        StringAppendF(&pending_info, "%ld,", pending_commit.first);
    }
    if (lock) _lock.unlock();

    std::string ret =
            strings::Substitute("tablet:$0 #version:$1 [$2 $3@$4 $5] pending:$6 rowsets:$7", _tablet.tablet_id(),
                                num_version, first_version.to_string(), apply_version.to_string(), apply_idx,
                                last_version.to_string(), pending_info, rowsets.size());
    _print_rowsets(rowsets, &ret, abbr);
    return ret;
}

std::string TabletUpdates::_debug_version_info(bool lock) const {
    size_t num_version;
    size_t apply_idx;
    EditVersion first_version;
    EditVersion apply_version;
    EditVersion last_version;
    size_t npending = 0;
    if (lock) _lock.lock();
    num_version = _edit_version_infos.size();
    // num_version can be 0, if clear_meta is called after deleting this Tablet
    if (num_version == 0) {
        if (lock) _lock.unlock();
        return strings::Substitute("tablet:$0 <deleted>", _tablet.tablet_id());
    }
    apply_idx = _apply_version_idx;
    first_version = _edit_version_infos[0]->version;
    apply_version = _edit_version_infos[_apply_version_idx]->version;
    last_version = _edit_version_infos.back()->version;
    npending = _pending_commits.size();
    if (lock) _lock.unlock();

    std::string ret = strings::Substitute("tablet:$0 #version:$1 [$2 $3@$4 $5] #pending:$6", _tablet.tablet_id(),
                                          num_version, first_version.to_string(), apply_version.to_string(), apply_idx,
                                          last_version.to_string(), npending);
    return ret;
}

void TabletUpdates::_print_rowsets(std::vector<uint32_t>& rowsets, std::string* dst, bool abbr) const {
    std::lock_guard rl(_rowset_stats_lock);
    if (abbr) {
        StringAppendF(dst, "[id/seg/row/del/byte/compaction]: ");
        for (int i = 0; i < rowsets.size(); i++) {
            if (i > 0) {
                dst->append(",");
            }
            auto rowsetid = rowsets[i];
            auto itr = _rowset_stats.find(rowsetid);
            if (itr != _rowset_stats.end()) {
                auto& stats = *itr->second;
                string bytes = PrettyPrinter::print(stats.byte_size, TUnit::BYTES);
                // PrettyPrinter doesn't support negative value
                string compaction = PrettyPrinter::print(std::abs(stats.compaction_score), TUnit::BYTES);
                const char* cprefix = "";
                if (stats.compaction_score < 0) {
                    cprefix = "-";
                }
                StringAppendF(dst, "[%d/%zu/%zu/%zu/%s/%s%s]", rowsetid, stats.num_segments, stats.num_rows,
                              stats.num_dels, bytes.c_str(), cprefix, compaction.c_str());
            } else {
                StringAppendF(dst, "[%d/NA]", rowsetid);
            }
            // only print fist 10 and last 10
            if (i == 10) {
                int newpos = std::max(i, (int)rowsets.size() - 10);
                if (newpos != i) {
                    StringAppendF(dst, "...");
                    i = newpos;
                }
            }
        }
    } else {
        for (uint32_t rowsetid : rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr != _rowset_stats.end()) {
                StringAppendF(dst, "\n  %u %s", rowsetid, itr->second->to_string().c_str());
            } else {
                StringAppendF(dst, "\n  %u NA", rowsetid);
            }
        }
    }
}

void TabletUpdates::_set_error(const string& msg) {
    _error_msg = msg;
    _error = true;
    _apply_version_changed.notify_all();
}

RowsetSharedPtr TabletUpdates::get_delta_rowset(int64_t version) const {
    if (_error) {
        LOG(WARNING) << strings::Substitute("get_delta_rowset failed, tablet updates is in error state: tablet:$0 $1",
                                            _tablet.tablet_id(), _error_msg);
        return nullptr;
    }
    std::lock_guard lg(_lock);
    if (_edit_version_infos.empty()) {
        LOG(WARNING) << "tablet deleted when get_delta_rowset tablet:" << _tablet.tablet_id();
        return nullptr;
    }
    if (version < _edit_version_infos[0]->version.major() || _edit_version_infos.back()->version.major() < version) {
        return nullptr;
    }
    int64_t idx_hint = version - _edit_version_infos[0]->version.major();
    for (auto i = idx_hint; i < _edit_version_infos.size(); i++) {
        const auto& vi = _edit_version_infos[i];
        if (vi->version.major() < version) {
            continue;
        }
        DCHECK_EQ(version, vi->version.major());
        if (vi->version.minor() != 0 || vi->deltas.empty()) {
            //                          ^^^^^^^^^^^^^^^^^ This may happen if this is a cloned version
            return nullptr;
        }
        DCHECK_EQ(1, vi->deltas.size());
        std::lock_guard lg2(_rowsets_lock);
        DCHECK_EQ(version, _rowsets.at(vi->deltas[0])->version().first);
        DCHECK_EQ(version, _rowsets.at(vi->deltas[0])->version().second);
        return _rowsets.at(vi->deltas[0]);
    }
    return nullptr;
}

Status TabletUpdates::get_applied_rowsets(int64_t version, std::vector<RowsetSharedPtr>* rowsets,
                                          EditVersion* full_edit_version) {
    if (_error) {
        return Status::InternalError(
                strings::Substitute("get_applied_rowsets failed, tablet updates is in error state: tablet:$0 $1",
                                    _tablet.tablet_id(), _error_msg));
    }
    std::unique_lock<std::mutex> ul(_lock);
    // wait for version timeout 55s, should smaller than exec_plan_fragment rpc timeout(60s)
    RETURN_IF_ERROR(_wait_for_version(EditVersion(version, 0), 55000, ul));
    if (_edit_version_infos.empty()) {
        string msg = strings::Substitute("tablet deleted when get_applied_rowsets tablet:$0", _tablet.tablet_id());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    for (ssize_t i = _apply_version_idx; i >= 0; i--) {
        const auto& edit_version_info = _edit_version_infos[i];
        if (edit_version_info->version.major() == version) {
            rowsets->reserve(edit_version_info->rowsets.size());
            std::lock_guard<std::mutex> lg(_rowsets_lock);
            for (uint32_t rsid : edit_version_info->rowsets) {
                auto itr = _rowsets.find(rsid);
                DCHECK(itr != _rowsets.end());
                if (itr != _rowsets.end()) {
                    rowsets->emplace_back(itr->second);
                } else {
                    string msg = strings::Substitute("get_rowsets rowset not found: version:$0 rowset:$1 $2", version,
                                                     rsid, _debug_string(false, true));
                    LOG(WARNING) << msg;
                    return Status::NotFound(msg);
                }
            }
            if (full_edit_version != nullptr) {
                *full_edit_version = edit_version_info->version;
            }
            return Status::OK();
        }
    }
    string msg = strings::Substitute("get_applied_rowsets(version $0) failed $1", version, _debug_version_info(false));
    LOG(WARNING) << msg;
    return Status::NotFound(msg);
}

struct RowsetLoadInfo {
    uint32_t rowset_id = 0;
    uint32_t num_segments = 0;
    RowsetMetaPB rowset_meta_pb;
    vector<DelVectorPtr> delvecs;
    vector<DeltaColumnGroupList> dcgs;
};

Status TabletUpdates::link_from(Tablet* base_tablet, int64_t request_version, ChunkChanger* chunk_changer,
                                const std::string& err_msg_header) {
    OlapStopWatch watch;
    DCHECK(_tablet.tablet_state() == TABLET_NOTREADY)
            << err_msg_header << "tablet state is not TABLET_NOTREADY, link_from is not allowed"
            << " tablet_id:" << _tablet.tablet_id() << " tablet_state:" << _tablet.tablet_state();
    LOG(INFO) << err_msg_header << "link_from start tablet:" << _tablet.tablet_id()
              << " #pending:" << _pending_commits.size() << " base_tablet:" << base_tablet->tablet_id()
              << " request_version:" << request_version;
    int64_t max_version = base_tablet->updates()->max_version();
    if (max_version < request_version) {
        LOG(WARNING) << err_msg_header << "link_from: base_tablet's max_version:" << max_version
                     << " < alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        return Status::InternalError("link_from: max_version < request version");
    }
    if (this->max_version() >= request_version) {
        LOG(WARNING) << err_msg_header << "link_from skipped: max_version:" << this->max_version()
                     << " >= alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        std::unique_lock wrlock(_tablet.get_header_lock());
        _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
        _tablet.save_meta();
        return Status::OK();
    }
    vector<RowsetSharedPtr> rowsets;
    EditVersion version;
    Status st = base_tablet->updates()->get_applied_rowsets(request_version, &rowsets, &version);
    if (!st.ok()) {
        LOG(WARNING) << err_msg_header << "link_from: get base tablet rowsets error tablet:" << base_tablet->tablet_id()
                     << " version:" << request_version << " reason:" << st;
        return st;
    }

    // disable compaction temporarily when tablet just loaded
    int64_t prev_last_compaction_time_ms = _last_compaction_time_ms;
    DeferOp op([&] { _last_compaction_time_ms = prev_last_compaction_time_ms; });
    _last_compaction_time_ms = UnixMillis();

    // 1. construct new rowsets
    auto kv_store = _tablet.data_dir()->get_meta();
    auto update_manager = StorageEngine::instance()->update_manager();
    auto tablet_id = _tablet.tablet_id();
    uint32_t next_rowset_id = 0;
    size_t total_bytes = 0;
    size_t total_rows = 0;
    size_t total_files = 0;
    vector<RowsetLoadInfo> new_rowsets(rowsets.size());
    for (int i = 0; i < rowsets.size(); i++) {
        auto& src_rowset = *rowsets[i];
        RowsetId rid = StorageEngine::instance()->next_rowset_id();
        auto st = src_rowset.link_files_to(base_tablet->data_dir()->get_meta(), _tablet.schema_hash_path(), rid,
                                           version.major());
        if (!st.ok()) {
            return st;
        }
        auto& new_rowset_info = new_rowsets[i];
        new_rowset_info.rowset_id = next_rowset_id;
        new_rowset_info.num_segments = src_rowset.num_segments();
        // use src_rowset's meta as base, change some fields to new tablet
        auto& rowset_meta_pb = new_rowset_info.rowset_meta_pb;
        src_rowset.rowset_meta()->to_rowset_pb(&rowset_meta_pb);
        rowset_meta_pb.set_deprecated_rowset_id(0);
        rowset_meta_pb.set_rowset_id(rid.to_string());
        rowset_meta_pb.set_rowset_seg_id(new_rowset_info.rowset_id);
        rowset_meta_pb.set_partition_id(_tablet.tablet_meta()->partition_id());
        rowset_meta_pb.set_tablet_id(tablet_id);
        rowset_meta_pb.set_tablet_schema_hash(_tablet.schema_hash());
        new_rowset_info.delvecs.resize(new_rowset_info.num_segments);
        new_rowset_info.dcgs.resize(new_rowset_info.num_segments);
        for (uint32_t j = 0; j < new_rowset_info.num_segments; j++) {
            TabletSegmentId tsid;
            tsid.tablet_id = src_rowset.rowset_meta()->tablet_id();
            tsid.segment_id = src_rowset.rowset_meta()->get_rowset_seg_id() + j;
            Status st = update_manager->get_del_vec(kv_store, tsid, version.major(), &new_rowset_info.delvecs[j]);
            if (!st.ok()) {
                return st;
            }
            st = update_manager->get_delta_column_group(kv_store, tsid, version.major(), &new_rowset_info.dcgs[j]);
            if (!st.ok()) {
                return st;
            }
        }

        // new added dcgs info for every segment in rowset.
        DeltaColumnGroupList dcgs;
        std::vector<int> last_dcg_counts;
        for (uint32_t j = 0; j < new_rowset_info.num_segments; j++) {
            // check the lastest historical_dcgs version if it is equal to schema change version
            // of the rowset. If it is, we should merge the dcg info.
            last_dcg_counts.emplace_back((new_rowset_info.dcgs[j].size() != 0 &&
                                          new_rowset_info.dcgs[j].front()->version() == version.major())
                                                 ? new_rowset_info.dcgs[j].front()->relative_column_files().size()
                                                 : 0);
        }
        RETURN_IF_ERROR(LinkedSchemaChange::generate_delta_column_group_and_cols(
                &_tablet, base_tablet, rowsets[i], rid, version.major(), chunk_changer, dcgs, last_dcg_counts));

        // merge dcg info if necessary
        if (dcgs.size() != 0) {
            if (dcgs.size() != new_rowset_info.num_segments) {
                std::stringstream ss;
                ss << "The size of dcgs and segment file in src rowset is different, "
                   << "base tablet id: " << base_tablet->tablet_id() << " "
                   << "new tablet id: " << _tablet.tablet_id();
                LOG(WARNING) << ss.str();
                return Status::InternalError(ss.str());
            }
            for (uint32_t j = 0; j < dcgs.size(); j++) {
                if (dcgs[j]->merge_into_by_version(new_rowset_info.dcgs[j], _tablet.schema_hash_path(), rid, j) == 0) {
                    // In this case, new_rowset_info.dcgs[j] contain no suitable dcg:
                    // 1. no version of dcg in new_rowset_info.dcgs[j] satisfy the request_version.
                    // 2. new_rowset_info.dcgs[j] is empty
                    // So nothing can be merged, and we should just insert the dcgs[j] into new_rowset_info.dcgs[j]
                    new_rowset_info.dcgs[j].insert(new_rowset_info.dcgs[j].begin(),
                                                   dcgs[j]); /* reverse order by version */
                }
            }
            rowset_meta_pb.set_partial_schema_change(true);
        }

        next_rowset_id += std::max(1U, (uint32_t)new_rowset_info.num_segments);
        total_bytes += rowset_meta_pb.total_disk_size();
        total_rows += rowset_meta_pb.num_rows();
        total_files += rowset_meta_pb.num_segments() + rowset_meta_pb.num_delete_files();
    }
    // 2. construct new meta
    TabletMetaPB meta_pb;
    _tablet.tablet_meta()->to_meta_pb(&meta_pb);
    meta_pb.set_tablet_state(TabletStatePB::PB_RUNNING);
    TabletUpdatesPB* updates_pb = meta_pb.mutable_updates();
    updates_pb->clear_versions();
    auto version_pb = updates_pb->add_versions();
    version_pb->mutable_version()->set_major(version.major());
    version_pb->mutable_version()->set_minor(version.minor());
    int64_t creation_time = time(nullptr);
    version_pb->set_creation_time(creation_time);
    for (auto& new_rowset : new_rowsets) {
        version_pb->mutable_rowsets()->Add(new_rowset.rowset_id);
    }
    version_pb->set_rowsetid_add(next_rowset_id);
    auto apply_version_pb = updates_pb->mutable_apply_version();
    apply_version_pb->set_major(version.major());
    apply_version_pb->set_minor(version.minor());
    updates_pb->set_next_log_id(1);
    updates_pb->set_next_rowset_id(next_rowset_id);

    // 3. delete old meta & write new meta
    auto data_dir = _tablet.data_dir();
    rocksdb::WriteBatch wb;
    RETURN_IF_ERROR(TabletMetaManager::clear_log(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_rowset(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_del_vector(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_delta_column_group(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_persistent_index(data_dir, &wb, tablet_id));
    // do not clear pending rowsets, because these pending rowsets should be committed after schemachange is done
    RETURN_IF_ERROR(TabletMetaManager::put_tablet_meta(data_dir, &wb, meta_pb));
    for (auto& info : new_rowsets) {
        RETURN_IF_ERROR(TabletMetaManager::put_rowset_meta(data_dir, &wb, tablet_id, info.rowset_meta_pb));
        for (int j = 0; j < info.num_segments; j++) {
            RETURN_IF_ERROR(
                    TabletMetaManager::put_del_vector(data_dir, &wb, tablet_id, info.rowset_id + j, *info.delvecs[j]));
            RETURN_IF_ERROR(TabletMetaManager::put_delta_column_group(data_dir, &wb, tablet_id, info.rowset_id + j,
                                                                      info.dcgs[j]));
        }
    }

    std::unique_lock wrlock(_tablet.get_header_lock());
    if (this->max_version() >= request_version) {
        LOG(WARNING) << err_msg_header << "link_from skipped: max_version:" << this->max_version()
                     << " >= alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
        _tablet.save_meta();
        return Status::OK();
    }
    st = kv_store->write_batch(&wb);
    if (!st.ok()) {
        LOG(WARNING) << err_msg_header << "Fail to delete old meta and write new meta" << tablet_id << ": " << st;
        return Status::InternalError("Fail to delete old meta and write new meta");
    }

    auto index_entry = update_manager->index_cache().get_or_create(tablet_id);
    index_entry->update_expire_time(MonotonicMillis() + update_manager->get_cache_expire_ms());
    auto& index = index_entry->value();
    index.unload();
    update_manager->index_cache().release(index_entry);
    // 4. load from new meta
    st = _load_from_pb(*updates_pb);
    if (!st.ok()) {
        LOG(WARNING) << err_msg_header << "_load_from_pb failed tablet_id:" << tablet_id << " " << st;
        return st;
    }
    _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
    LOG(INFO) << err_msg_header << "link_from finish tablet:" << _tablet.tablet_id()
              << " version:" << this->max_version() << " base tablet:" << base_tablet->tablet_id()
              << " #pending:" << _pending_commits.size() << " time:" << watch.get_elapse_second() << "s"
              << " #rowset:" << rowsets.size() << " #file:" << total_files << " #row:" << total_rows
              << " bytes:" << total_bytes;
    return Status::OK();
}

Status TabletUpdates::convert_from(const std::shared_ptr<Tablet>& base_tablet, int64_t request_version,
                                   ChunkChanger* chunk_changer, const std::string& err_msg_header) {
    OlapStopWatch watch;
    DCHECK(_tablet.tablet_state() == TABLET_NOTREADY)
            << err_msg_header << "tablet state is not TABLET_NOTREADY, convert_from is not allowed"
            << " tablet_id:" << _tablet.tablet_id() << " tablet_state:" << _tablet.tablet_state();
    LOG(INFO) << err_msg_header << "convert_from start tablet:" << _tablet.tablet_id()
              << " #pending:" << _pending_commits.size() << " base_tablet:" << base_tablet->tablet_id()
              << " request_version:" << request_version;
    int64_t max_version = base_tablet->updates()->max_version();
    if (max_version < request_version) {
        LOG(WARNING) << err_msg_header << "convert_from: base_tablet's max_version:" << max_version
                     << " < alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        return Status::InternalError("convert_from: max_version < request_version");
    }
    if (this->max_version() >= request_version) {
        LOG(WARNING) << err_msg_header << "convert_from skipped: max_version:" << this->max_version()
                     << " >= alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        std::unique_lock wrlock(_tablet.get_header_lock());
        _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
        _tablet.save_meta();
        return Status::OK();
    }
    std::vector<RowsetSharedPtr> src_rowsets;
    EditVersion version;
    Status status = base_tablet->updates()->get_applied_rowsets(request_version, &src_rowsets, &version);
    if (!status.ok()) {
        LOG(WARNING) << err_msg_header
                     << "convert_from: get base tablet rowsets error tablet:" << base_tablet->tablet_id()
                     << " request_version:" << request_version << " reason:" << status;
        return status;
    }

    // disable compaction temporarily when tablet just loaded
    int64_t prev_last_compaction_time_ms = _last_compaction_time_ms;
    DeferOp op([&] { _last_compaction_time_ms = prev_last_compaction_time_ms; });
    _last_compaction_time_ms = UnixMillis();

    auto kv_store = _tablet.data_dir()->get_meta();
    auto tablet_id = _tablet.tablet_id();
    uint32_t next_rowset_id = 0;
    std::vector<RowsetLoadInfo> new_rowset_load_infos(src_rowsets.size());

    Schema base_schema = ChunkHelper::convert_schema(base_tablet->tablet_schema());

    OlapReaderStatistics stats;

    size_t total_bytes = 0;
    size_t total_rows = 0;
    size_t total_files = 0;
    for (int i = 0; i < src_rowsets.size(); i++) {
        const auto& src_rowset = src_rowsets[i];

        RowsetReleaseGuard guard(src_rowset->shared_from_this());
        auto res = src_rowset->get_segment_iterators2(base_schema, base_tablet->data_dir()->get_meta(), version.major(),
                                                      &stats);
        if (!res.ok()) {
            return res.status();
        }

        RowsetId rid = StorageEngine::instance()->next_rowset_id();

        RowsetWriterContext writer_context;
        writer_context.rowset_id = rid;
        writer_context.tablet_uid = _tablet.tablet_uid();
        writer_context.tablet_id = _tablet.tablet_id();
        writer_context.partition_id = _tablet.partition_id();
        writer_context.tablet_schema_hash = _tablet.schema_hash();
        writer_context.rowset_path_prefix = _tablet.schema_hash_path();
        writer_context.tablet_schema = &_tablet.tablet_schema();
        writer_context.rowset_state = VISIBLE;
        writer_context.version = src_rowset->version();
        writer_context.segments_overlap = NONOVERLAPPING;

        std::unique_ptr<RowsetWriter> rowset_writer;
        status = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        if (!status.ok()) {
            LOG(INFO) << err_msg_header << "build rowset writer failed";
            return Status::InternalError("build rowset writer failed");
        }
        ChunkIteratorPtr seg_iterator;
        if (res.value().empty()) {
            seg_iterator = new_empty_iterator(base_schema, config::vector_chunk_size);
        } else {
            if (src_rowset->rowset_meta()->is_segments_overlapping()) {
                seg_iterator = new_heap_merge_iterator(res.value());
            } else {
                seg_iterator = new_union_iterator(res.value());
            }
        }

        // notice: rowset's del files not linked, it's not useful
        status = _convert_from_base_rowset(base_tablet, seg_iterator, chunk_changer, rowset_writer);
        if (!status.ok()) {
            LOG(WARNING) << err_msg_header << "failed to convert from base rowset, exit alter process, "
                         << status.to_string();
            return status;
        }

        auto new_rowset = rowset_writer->build();
        if (!new_rowset.ok()) return new_rowset.status();

        if (config::enable_rowset_verify) {
            status = (*new_rowset)->verify();
            if (!status.ok()) {
                status = status.clone_and_append(strings::Substitute("$0 convert_from base_tablet: $1", err_msg_header,
                                                                     base_tablet->tablet_id()));
                LOG(WARNING) << status.get_error_msg();
                return status;
            }
        }
        auto& new_rowset_load_info = new_rowset_load_infos[i];
        new_rowset_load_info.num_segments = (*new_rowset)->num_segments();
        new_rowset_load_info.rowset_id = next_rowset_id;

        auto& rowset_meta_pb = new_rowset_load_info.rowset_meta_pb;
        (*new_rowset)->rowset_meta()->to_rowset_pb(&rowset_meta_pb);
        rowset_meta_pb.set_rowset_seg_id(new_rowset_load_info.rowset_id);
        rowset_meta_pb.set_rowset_id(rid.to_string());

        next_rowset_id += std::max(1U, (uint32_t)new_rowset_load_info.num_segments);

        total_bytes += rowset_meta_pb.total_disk_size();
        total_rows += rowset_meta_pb.num_rows();
        total_files += rowset_meta_pb.num_segments() + rowset_meta_pb.num_delete_files();
    }

    TabletMetaPB meta_pb;
    _tablet.tablet_meta()->to_meta_pb(&meta_pb);
    meta_pb.set_tablet_state(TabletStatePB::PB_RUNNING);
    TabletUpdatesPB* updates_pb = meta_pb.mutable_updates();
    updates_pb->clear_versions();
    auto version_pb = updates_pb->add_versions();
    version_pb->mutable_version()->set_major(version.major());
    version_pb->mutable_version()->set_minor(version.minor());
    int64_t creation_time = time(nullptr);
    version_pb->set_creation_time(creation_time);
    for (auto& new_rowset_load_info : new_rowset_load_infos) {
        version_pb->mutable_rowsets()->Add(new_rowset_load_info.rowset_id);
    }
    version_pb->set_rowsetid_add(next_rowset_id);
    auto apply_version_pb = updates_pb->mutable_apply_version();
    apply_version_pb->set_major(version.major());
    apply_version_pb->set_minor(version.minor());
    updates_pb->set_next_log_id(1);
    updates_pb->set_next_rowset_id(next_rowset_id);

    // delete old meta & write new meta
    auto data_dir = _tablet.data_dir();
    rocksdb::WriteBatch wb;
    RETURN_IF_ERROR(TabletMetaManager::clear_log(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_rowset(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_del_vector(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_delta_column_group(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_persistent_index(data_dir, &wb, tablet_id));
    // do not clear pending rowsets, because these pending rowsets should be committed after schemachange is done
    RETURN_IF_ERROR(TabletMetaManager::put_tablet_meta(data_dir, &wb, meta_pb));
    DelVector delvec;
    for (const auto& new_rowset_load_info : new_rowset_load_infos) {
        RETURN_IF_ERROR(
                TabletMetaManager::put_rowset_meta(data_dir, &wb, tablet_id, new_rowset_load_info.rowset_meta_pb));
        for (int j = 0; j < new_rowset_load_info.num_segments; j++) {
            RETURN_IF_ERROR(TabletMetaManager::put_del_vector(data_dir, &wb, tablet_id,
                                                              new_rowset_load_info.rowset_id + j, delvec));
        }
    }

    std::unique_lock wrlock(_tablet.get_header_lock());
    if (this->max_version() >= request_version) {
        LOG(WARNING) << err_msg_header << "convert_from skipped: max_version:" << this->max_version()
                     << " >= alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
        _tablet.save_meta();
        return Status::OK();
    }
    status = kv_store->write_batch(&wb);
    if (!status.ok()) {
        LOG(WARNING) << err_msg_header << "Fail to delete old meta and write new meta" << tablet_id << ": " << status;
        return Status::InternalError(err_msg_header + "Fail to delete old meta and write new meta");
    }

    auto update_manager = StorageEngine::instance()->update_manager();
    auto index_entry = update_manager->index_cache().get_or_create(tablet_id);
    index_entry->update_expire_time(MonotonicMillis() + update_manager->get_cache_expire_ms());
    auto& index = index_entry->value();
    index.unload();
    update_manager->index_cache().release(index_entry);
    // 4. load from new meta
    status = _load_from_pb(*updates_pb);
    if (!status.ok()) {
        LOG(WARNING) << err_msg_header << "_load_from_pb failed tablet_id:" << tablet_id << " " << status;
        return status;
    }

    _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
    LOG(INFO) << err_msg_header << "convert_from finish tablet:" << _tablet.tablet_id()
              << " version:" << this->max_version() << " base tablet:" << base_tablet->tablet_id()
              << " #pending:" << _pending_commits.size() << " time:" << watch.get_elapse_second() << "s"
              << " #column:" << _tablet.tablet_schema().num_columns() << " #rowset:" << src_rowsets.size()
              << " #file:" << total_files << " #row:" << total_rows << " bytes:" << total_bytes;
    ;
    return Status::OK();
}

Status TabletUpdates::_convert_from_base_rowset(const std::shared_ptr<Tablet>& base_tablet,
                                                const ChunkIteratorPtr& seg_iterator, ChunkChanger* chunk_changer,
                                                const std::unique_ptr<RowsetWriter>& rowset_writer) {
    Schema base_schema = ChunkHelper::convert_schema(base_tablet->tablet_schema());
    ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);

    Schema new_schema = ChunkHelper::convert_schema(_tablet.tablet_schema());
    ChunkPtr new_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);

    std::unique_ptr<MemPool> mem_pool(new MemPool());

    if (seg_iterator.get() != nullptr) {
        while (true) {
            base_chunk->reset();
            new_chunk->reset();
            mem_pool->clear();
            Status status = seg_iterator->get_next(base_chunk.get());
            if (!status.ok()) {
                if (status.is_end_of_file()) {
                    break;
                } else {
                    std::stringstream ss;
                    ss << "segment iterator failed to get next chunk, status is:" << status.to_string();
                    LOG(WARNING) << ss.str();
                    return Status::InternalError(ss.str());
                }
            }
            if (!chunk_changer->change_chunk(base_chunk, new_chunk, base_tablet->tablet_meta(), _tablet.tablet_meta(),
                                             mem_pool.get())) {
                LOG(WARNING) << "failed to change data in chunk";
                return Status::InternalError("failed to change data in chunk");
            }
            status = chunk_changer->fill_materialized_columns(new_chunk);
            if (!status.ok()) {
                LOG(WARNING) << "failed to fill materialized column";
                return Status::InternalError("failed to fill materialized column");
            }
            RETURN_IF_ERROR(rowset_writer->add_chunk(*new_chunk));
        }
    }

    return rowset_writer->flush();
}

Status TabletUpdates::reorder_from(const std::shared_ptr<Tablet>& base_tablet, int64_t request_version,
                                   ChunkChanger* chunk_changer, const std::string& err_msg_header) {
    OlapStopWatch watch;
    DCHECK(_tablet.tablet_state() == TABLET_NOTREADY)
            << err_msg_header << "tablet state is not TABLET_NOTREADY, reorder_from is not allowed"
            << " tablet_id:" << _tablet.tablet_id() << " tablet_state:" << _tablet.tablet_state();
    LOG(INFO) << err_msg_header << "reorder_from start tablet:" << _tablet.tablet_id()
              << " #pending:" << _pending_commits.size() << " base_tablet:" << base_tablet->tablet_id()
              << " request_version:" << request_version;
    int64_t max_version = base_tablet->updates()->max_version();
    if (max_version < request_version) {
        LOG(WARNING) << err_msg_header << "reorder_from: base_tablet's max_version:" << max_version
                     << " < alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        return Status::InternalError("reorder_from: max_version < request_version");
    }
    if (this->max_version() >= request_version) {
        LOG(WARNING) << err_msg_header << "reorder_from skipped: max_version:" << this->max_version()
                     << " >= alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        std::unique_lock wrlock(_tablet.get_header_lock());
        _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
        _tablet.save_meta();
        return Status::OK();
    }
    std::vector<RowsetSharedPtr> src_rowsets;
    EditVersion version;
    Status status = base_tablet->updates()->get_applied_rowsets(request_version, &src_rowsets, &version);
    if (!status.ok()) {
        LOG(WARNING) << err_msg_header
                     << "reorder_from: get base tablet rowsets error tablet:" << base_tablet->tablet_id()
                     << " request_version:" << request_version << " reason:" << status;
        return status;
    }
    std::unique_ptr<MemPool> mem_pool(new MemPool());

    // disable compaction temporarily when tablet just loaded
    int64_t prev_last_compaction_time_ms = _last_compaction_time_ms;
    DeferOp op([&] { _last_compaction_time_ms = prev_last_compaction_time_ms; });
    _last_compaction_time_ms = UnixMillis();

    auto kv_store = _tablet.data_dir()->get_meta();
    auto tablet_id = _tablet.tablet_id();
    uint32_t next_rowset_id = 0;
    std::vector<RowsetLoadInfo> new_rowset_load_infos(src_rowsets.size());

    std::vector<ChunkPtr> chunk_arr;

    Schema base_schema = ChunkHelper::convert_schema(base_tablet->tablet_schema());
    ChunkSorter chunk_sorter;

    OlapReaderStatistics stats;

    size_t total_bytes = 0;
    size_t total_rows = 0;
    size_t total_files = 0;
    for (int i = 0; i < src_rowsets.size(); i++) {
        const auto& src_rowset = src_rowsets[i];

        RowsetReleaseGuard guard(src_rowset->shared_from_this());
        auto res = src_rowset->get_segment_iterators2(base_schema, base_tablet->data_dir()->get_meta(), version.major(),
                                                      &stats);
        if (!res.ok()) {
            return res.status();
        }
        const auto& seg_iterators = res.value();

        RowsetId rid = StorageEngine::instance()->next_rowset_id();

        RowsetWriterContext writer_context;
        writer_context.rowset_id = rid;
        writer_context.tablet_uid = _tablet.tablet_uid();
        writer_context.tablet_id = _tablet.tablet_id();
        writer_context.partition_id = _tablet.partition_id();
        writer_context.tablet_schema_hash = _tablet.schema_hash();
        writer_context.rowset_path_prefix = _tablet.schema_hash_path();
        writer_context.tablet_schema = &_tablet.tablet_schema();
        writer_context.rowset_state = VISIBLE;
        writer_context.version = src_rowset->version();
        writer_context.segments_overlap = src_rowset->rowset_meta()->segments_overlap();
        writer_context.schema_change_sorting = true;

        std::unique_ptr<RowsetWriter> rowset_writer;
        status = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        if (!status.ok()) {
            LOG(INFO) << err_msg_header << "build rowset writer failed";
            return Status::InternalError(err_msg_header + "build rowset writer failed");
        }

        ChunkPtr base_chunk = ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);

        Schema new_schema = ChunkHelper::convert_schema(_tablet.tablet_schema());
        ChunkPtr new_chunk = ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);

        for (auto& seg_iterator : seg_iterators) {
            if (seg_iterator.get() == nullptr) {
                continue;
            }
            while (true) {
                base_chunk->reset();
                Status status = seg_iterator->get_next(base_chunk.get());
                if (!status.ok()) {
                    if (status.is_end_of_file()) {
                        break;
                    } else {
                        std::stringstream ss;
                        ss << err_msg_header
                           << "segment iterator failed to get next chunk, status is:" << status.to_string();
                        LOG(WARNING) << ss.str();
                        return Status::InternalError(ss.str());
                    }
                }

                if (!chunk_changer->change_chunk_v2(base_chunk, new_chunk, base_schema, new_schema, mem_pool.get())) {
                    std::string err_msg =
                            strings::Substitute("failed to convert chunk data. base tablet:$0, new tablet:$1",
                                                base_tablet->tablet_id(), _tablet.tablet_id());
                    LOG(WARNING) << err_msg_header << err_msg;
                    return Status::InternalError(err_msg_header + err_msg);
                }

                total_bytes += static_cast<double>(new_chunk->memory_usage());
                total_rows += static_cast<double>(new_chunk->num_rows());

                if (new_chunk->num_rows() > 0) {
                    if (!chunk_sorter.sort(new_chunk, std::static_pointer_cast<Tablet>(_tablet.shared_from_this()))) {
                        LOG(WARNING) << err_msg_header << "chunk data sort failed";
                        return Status::InternalError(err_msg_header + "chunk data sort failed");
                    }
                }
                chunk_arr.push_back(new_chunk);
            }
        }

        if (!chunk_arr.empty()) {
            Status st = SchemaChangeWithSorting::_internal_sorting(
                    chunk_arr, rowset_writer.get(), std::static_pointer_cast<Tablet>(_tablet.shared_from_this()));
            if (!st.ok()) {
                std::string msg =
                        err_msg_header + strings::Substitute("failed to sorting internally, {}", st.to_string());
                return Status::InternalError(msg);
            }
        }

        status = rowset_writer->flush();
        if (!status.ok()) {
            LOG(WARNING) << err_msg_header << "failed to convert from base rowset, exit alter process";
            return status;
        }

        auto new_rowset = rowset_writer->build();
        if (!new_rowset.ok()) return new_rowset.status();

        if (config::enable_rowset_verify) {
            status = (*new_rowset)->verify();
            if (!status.ok()) {
                status = status.clone_and_append(strings::Substitute("$0 reorder_from base_tablet: $1", err_msg_header,
                                                                     base_tablet->tablet_id()));
                LOG(WARNING) << status.get_error_msg();
                return status;
            }
        }

        auto& new_rowset_load_info = new_rowset_load_infos[i];
        new_rowset_load_info.num_segments = (*new_rowset)->num_segments();
        new_rowset_load_info.rowset_id = next_rowset_id;

        auto& rowset_meta_pb = new_rowset_load_info.rowset_meta_pb;
        (*new_rowset)->rowset_meta()->to_rowset_pb(&rowset_meta_pb);
        rowset_meta_pb.set_rowset_seg_id(new_rowset_load_info.rowset_id);
        rowset_meta_pb.set_rowset_id(rid.to_string());

        next_rowset_id += std::max(1U, (uint32_t)new_rowset_load_info.num_segments);

        total_bytes += rowset_meta_pb.total_disk_size();
        total_rows += rowset_meta_pb.num_rows();
        total_files += rowset_meta_pb.num_segments() + rowset_meta_pb.num_delete_files();
        chunk_arr.clear();
    }

    TabletMetaPB meta_pb;
    _tablet.tablet_meta()->to_meta_pb(&meta_pb);
    meta_pb.set_tablet_state(TabletStatePB::PB_RUNNING);
    TabletUpdatesPB* updates_pb = meta_pb.mutable_updates();
    updates_pb->clear_versions();
    auto version_pb = updates_pb->add_versions();
    version_pb->mutable_version()->set_major(version.major());
    version_pb->mutable_version()->set_minor(version.minor());
    int64_t creation_time = time(nullptr);
    version_pb->set_creation_time(creation_time);
    for (auto& new_rowset_load_info : new_rowset_load_infos) {
        version_pb->mutable_rowsets()->Add(new_rowset_load_info.rowset_id);
    }
    version_pb->set_rowsetid_add(next_rowset_id);
    auto apply_version_pb = updates_pb->mutable_apply_version();
    apply_version_pb->set_major(version.major());
    apply_version_pb->set_minor(version.minor());
    updates_pb->set_next_log_id(1);
    updates_pb->set_next_rowset_id(next_rowset_id);

    // delete old meta & write new meta
    auto data_dir = _tablet.data_dir();
    rocksdb::WriteBatch wb;
    RETURN_IF_ERROR(TabletMetaManager::clear_log(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_rowset(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_del_vector(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_delta_column_group(data_dir, &wb, tablet_id));
    RETURN_IF_ERROR(TabletMetaManager::clear_persistent_index(data_dir, &wb, tablet_id));
    // do not clear pending rowsets, because these pending rowsets should be committed after schemachange is done
    RETURN_IF_ERROR(TabletMetaManager::put_tablet_meta(data_dir, &wb, meta_pb));
    DelVector delvec;
    for (const auto& new_rowset_load_info : new_rowset_load_infos) {
        RETURN_IF_ERROR(
                TabletMetaManager::put_rowset_meta(data_dir, &wb, tablet_id, new_rowset_load_info.rowset_meta_pb));
        for (int j = 0; j < new_rowset_load_info.num_segments; j++) {
            RETURN_IF_ERROR(TabletMetaManager::put_del_vector(data_dir, &wb, tablet_id,
                                                              new_rowset_load_info.rowset_id + j, delvec));
        }
    }

    std::unique_lock wrlock(_tablet.get_header_lock());
    if (this->max_version() >= request_version) {
        LOG(WARNING) << err_msg_header << "reorder_from skipped: max_version:" << this->max_version()
                     << " >= alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
        _tablet.save_meta();
        return Status::OK();
    }
    status = kv_store->write_batch(&wb);
    if (!status.ok()) {
        LOG(WARNING) << err_msg_header << "Fail to delete old meta and write new meta" << tablet_id << ": " << status;
        return Status::InternalError(err_msg_header + "Fail to delete old meta and write new meta");
    }

    auto update_manager = StorageEngine::instance()->update_manager();
    auto index_entry = update_manager->index_cache().get_or_create(tablet_id);
    index_entry->update_expire_time(MonotonicMillis() + update_manager->get_cache_expire_ms());
    auto& index = index_entry->value();
    index.unload();
    update_manager->index_cache().release(index_entry);
    // 4. load from new meta
    status = _load_from_pb(*updates_pb);
    if (!status.ok()) {
        LOG(WARNING) << err_msg_header << "_load_from_pb failed tablet_id:" << tablet_id << " " << status;
        return status;
    }

    _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
    LOG(INFO) << err_msg_header << "reorder_from finish tablet:" << _tablet.tablet_id()
              << " version:" << this->max_version() << " base tablet:" << base_tablet->tablet_id()
              << " #pending:" << _pending_commits.size() << " time:" << watch.get_elapse_second() << "s"
              << " #column:" << _tablet.tablet_schema().num_columns() << " #rowset:" << src_rowsets.size()
              << " #file:" << total_files << " #row:" << total_rows << " bytes:" << total_bytes;
    return Status::OK();
}

void TabletUpdates::_remove_unused_rowsets(bool drop_tablet) {
    size_t removed = 0;
    std::vector<RowsetSharedPtr> skipped_rowsets;
    RowsetSharedPtr rowset;
    while (_unused_rowsets.try_get(&rowset) == 1) {
        if (rowset.use_count() > 1) {
            if (drop_tablet) {
                LOG(WARNING) << "rowset " << rowset->rowset_id() << " still been referenced"
                             << " tablet:" << _tablet.tablet_id() << " rowset_id:" << rowset->rowset_id().id()
                             << " use_count: " << rowset.use_count() << " refs_by_reader:" << rowset->refs_by_reader()
                             << " version:" << rowset->version();
            } else {
                VLOG(1) << "rowset " << rowset->rowset_id() << " still been referenced"
                        << " tablet:" << _tablet.tablet_id() << " rowset_id:" << rowset->rowset_id().id()
                        << " use_count: " << rowset.use_count() << " refs_by_reader:" << rowset->refs_by_reader()
                        << " version:" << rowset->version();
            }

            skipped_rowsets.emplace_back(std::move(rowset));
            continue;
        }

        _clear_rowset_del_vec_cache(*rowset);
        _clear_rowset_delta_column_group_cache(*rowset);

        Status st = rowset->remove_delta_column_group(_tablet.data_dir()->get_meta());
        if (!st.ok()) {
            LOG(WARNING) << "Fail to delete delta column group. err: " << st.get_error_msg()
                         << ", rowset_id: " << rowset->rowset_id() << ", tablet_id: " << _tablet.tablet_id();
            skipped_rowsets.emplace_back(std::move(rowset));
            continue;
        }

        st = TabletMetaManager::rowset_delete(_tablet.data_dir(), _tablet.tablet_id(),
                                              rowset->rowset_meta()->get_rowset_seg_id(), rowset->num_segments());
        if (!st.ok()) {
            LOG(WARNING) << "Fail to delete rowset " << rowset->rowset_id() << ": " << st
                         << " tablet:" << _tablet.tablet_id();
            skipped_rowsets.emplace_back(std::move(rowset));
            continue;
        }
        rowset->close();
        rowset->set_need_delete_file();
        StorageEngine::instance()->release_rowset_id(rowset->rowset_id());
        auto ost = rowset->remove();
        VLOG(1) << "remove rowset " << _tablet.tablet_id() << "@" << rowset->rowset_meta()->get_rowset_seg_id() << "@"
                << rowset->rowset_id() << ": " << ost << " tablet:" << _tablet.tablet_id();
        removed++;
    }
    for (auto& r : skipped_rowsets) {
        _unused_rowsets.blocking_put(std::move(r));
    }
    if (removed > 0) {
        LOG(INFO) << "_remove_unused_rowsets remove " << removed << " rowsets, tablet:" << _tablet.tablet_id();
    }
}

Status TabletUpdates::check_and_remove_rowset() {
    _remove_unused_rowsets(true);
    if (!_unused_rowsets.empty()) {
        std::string msg =
                strings::Substitute("some rowsets of tablet: $0 are still been referenced", _tablet.tablet_id());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    return Status::OK();
}

void TabletUpdates::get_basic_info_extra(TabletBasicInfo& info) {
    vector<uint32_t> rowsets;
    {
        std::lock_guard rl(_lock);
        if (_edit_version_infos.empty()) {
            LOG(WARNING) << "tablet delete when get_tablet_info_extra tablet:" << _tablet.tablet_id();
            return;
        }
        info.num_version = _edit_version_infos.size();
        info.min_version = _edit_version_infos[0]->version.major();
        auto& v = _edit_version_infos[_edit_version_infos.size() - 1];
        info.max_version = v->version.major();
        info.num_rowset = v->rowsets.size();
        rowsets = v->rowsets;
    }
    int64_t total_row = 0;
    int64_t total_size = 0;
    {
        std::lock_guard lg(_rowset_stats_lock);
        for (uint32_t rowsetid : rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr != _rowset_stats.end()) {
                // TODO(cbl): also report num deletes
                total_row += itr->second->num_rows;
                total_size += itr->second->byte_size;
            }
        }
    }
    info.num_row = total_row;
    info.data_size = total_size;
    auto& index_cache = StorageEngine::instance()->update_manager()->index_cache();
    auto index_entry = index_cache.get(_tablet.tablet_id());
    if (index_entry != nullptr) {
        info.index_mem = index_entry->size();
        index_cache.release(index_entry);
    }
}

void TabletUpdates::_to_updates_pb_unlocked(TabletUpdatesPB* updates_pb) const {
    updates_pb->Clear();
    for (const auto& version : _edit_version_infos) {
        EditVersionMetaPB* version_pb = updates_pb->add_versions();
        // version
        version_pb->mutable_version()->set_major(version->version.major());
        version_pb->mutable_version()->set_minor(version->version.minor());
        // creation_time
        version_pb->set_creation_time(version->creation_time);
        // rowsets
        repeated_field_add(version_pb->mutable_rowsets(), version->rowsets.begin(), version->rowsets.end());
        // deltas
        repeated_field_add(version_pb->mutable_deltas(), version->deltas.begin(), version->deltas.end());
        // compaction
        if (version->compaction) {
            auto cp = version->compaction.get();
            auto compaction_pb = version_pb->mutable_compaction();
            repeated_field_add(compaction_pb->mutable_inputs(), cp->inputs.begin(), cp->inputs.end());
            compaction_pb->add_outputs(cp->output);
            auto svpb = compaction_pb->mutable_start_version();
            svpb->set_major(cp->start_version.major());
            svpb->set_minor(cp->start_version.minor());
        }
        // rowsetid_add is only useful in meta log, and it's a bit harder to construct it
        // so do not set it here
    }
    updates_pb->set_next_rowset_id(_next_rowset_id);
    updates_pb->set_next_log_id(_next_log_id);
    if (_apply_version_idx < _edit_version_infos.size()) {
        const EditVersion& apply_version = _edit_version_infos[_apply_version_idx]->version;
        updates_pb->mutable_apply_version()->set_major(apply_version.major());
        updates_pb->mutable_apply_version()->set_minor(apply_version.minor());
    }
}

Status TabletUpdates::load_snapshot(const SnapshotMeta& snapshot_meta, bool restore_from_backup) {
#define CHECK_FAIL(status)                                                                       \
    do {                                                                                         \
        Status st = (status);                                                                    \
        if (!st.ok()) {                                                                          \
            auto msg = strings::Substitute("$0 tablet:$1", st.to_string(), _tablet.tablet_id()); \
            LOG(ERROR) << msg;                                                                   \
            _set_error(msg);                                                                     \
            return st;                                                                           \
        }                                                                                        \
    } while (0)

    if (_error.load()) {
        return Status::InternalError(
                strings::Substitute("load snapshot failed, tablet updates is in error state: tablet:$0 $1",
                                    _tablet.tablet_id(), _error_msg));
    }
    // disable compaction temporarily when doing load_snapshot
    int64_t prev_last_compaction_time_ms = _last_compaction_time_ms;
    DeferOp op([&] { _last_compaction_time_ms = prev_last_compaction_time_ms; });
    _last_compaction_time_ms = UnixMillis();

    // A utility function used to ensure that segment files have been placed under the
    // tablet directory.
    auto check_rowset_files = [&](const RowsetMetaPB& rowset) {
        for (int seg_id = 0; seg_id < rowset.num_segments(); seg_id++) {
            RowsetId rowset_id;
            rowset_id.init(rowset.rowset_id());
            auto path = Rowset::segment_file_path(_tablet.schema_hash_path(), rowset_id, seg_id);
            auto st = FileSystem::Default()->path_exists(path);
            if (!st.ok()) {
                return Status::InternalError("segment file does not exist: " + st.to_string());
            }
        }
        for (int del_id = 0; del_id < rowset.num_delete_files(); del_id++) {
            RowsetId rowset_id;
            rowset_id.init(rowset.rowset_id());
            auto path = Rowset::segment_del_file_path(_tablet.schema_hash_path(), rowset_id, del_id);
            auto st = FileSystem::Default()->path_exists(path);
            if (!st.ok()) {
                return Status::InternalError("delete file does not exist: " + st.to_string());
            }
        }
        for (int upt_id = 0; upt_id < rowset.num_update_files(); upt_id++) {
            RowsetId rowset_id;
            rowset_id.init(rowset.rowset_id());
            auto path = Rowset::segment_upt_file_path(_tablet.schema_hash_path(), rowset_id, upt_id);
            auto st = FileSystem::Default()->path_exists(path);
            if (!st.ok()) {
                return Status::InternalError("update file does not exist: " + st.to_string());
            }
        }
        return Status::OK();
    };

    if (snapshot_meta.snapshot_type() == SNAPSHOT_TYPE_INCREMENTAL) {
        // Assume the elements of |snapshot_meta.rowset_metas()| are sorted by version.
        LOG(INFO) << "load incremental snapshot start #rowset:" << snapshot_meta.rowset_metas().size() << " "
                  << _debug_string(true);
        for (const auto& rowset_meta_pb : snapshot_meta.rowset_metas()) {
            RETURN_IF_ERROR(check_rowset_files(rowset_meta_pb));
            RowsetSharedPtr rowset;
            auto rowset_meta = std::make_shared<RowsetMeta>(rowset_meta_pb);
            if (rowset_meta->tablet_id() != _tablet.tablet_id()) {
                return Status::InternalError("mismatched tablet id");
            }
            RETURN_IF_ERROR(RowsetFactory::create_rowset(&_tablet.tablet_schema(), _tablet.schema_hash_path(),
                                                         rowset_meta, &rowset));
            if (rowset->start_version() != rowset->end_version()) {
                return Status::InternalError("mismatched start and end version");
            }
            RETURN_IF_ERROR(rowset_commit(rowset->end_version(), rowset, 0));
        }
        LOG(INFO) << "load incremental snapshot done " << _debug_string(true);
        return Status::OK();
    } else if (snapshot_meta.snapshot_type() == SNAPSHOT_TYPE_FULL) {
        LOG(INFO) << "load full snapshot start #rowset:" << snapshot_meta.rowset_metas().size()
                  << " version:" << snapshot_meta.snapshot_version() << " " << _debug_string(true);
        if (snapshot_meta.tablet_meta().tablet_id() != _tablet.tablet_id()) {
            return Status::InvalidArgument("mismatched tablet id");
        }
        if (snapshot_meta.tablet_meta().schema_hash() != _tablet.schema_hash()) {
            return Status::InvalidArgument("mismatched schema hash");
        }
        // If it is the Restore process, we should skip the version check because using the older version data
        // to overwrite the current data is permitted in Restore process.
        if (!restore_from_backup && snapshot_meta.snapshot_version() <= _edit_version_infos.back()->version.major()) {
            return Status::Cancelled("snapshot version too small");
        }
        for (const auto& rowset_meta_pb : snapshot_meta.rowset_metas()) {
            RETURN_IF_ERROR(check_rowset_files(rowset_meta_pb));
        }
        // Stop apply thread.
        _stop_and_wait_apply_done();

        DeferOp defer([&]() {
            if (!_error.load()) {
                // Start apply thread again.
                _apply_stopped.store(false);
                _check_for_apply();
            }
        });

        auto tablet_id = _tablet.tablet_id();
        auto data_store = _tablet.data_dir();
        auto meta_store = data_store->get_meta();

        std::unordered_map<uint32_t, RowsetSharedPtr> new_rowsets;

        std::lock_guard l0(_tablet.get_header_lock());

        TabletMetaPB new_tablet_meta_pb;
        _tablet.tablet_meta()->to_meta_pb(&new_tablet_meta_pb);

        std::unique_lock l1(_lock);
        if (_edit_version_infos.empty()) {
            string msg = strings::Substitute("tablet deleted when load_snapshot tablet:$0", _tablet.tablet_id());
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        std::unique_lock l2(_rowsets_lock);
        std::unique_lock l3(_rowset_stats_lock);

        // Check version again after lock acquired.
        if (!restore_from_backup && snapshot_meta.snapshot_version() <= _edit_version_infos.back()->version.major()) {
            return Status::Cancelled("snapshot version too small");
        }

        uint32_t new_next_rowset_id = _next_rowset_id;
        for (const auto& rowset_meta_pb : snapshot_meta.rowset_metas()) {
            auto rowset_meta = std::make_shared<RowsetMeta>(rowset_meta_pb);
            const auto new_id = rowset_meta_pb.rowset_seg_id() + _next_rowset_id;
            new_next_rowset_id =
                    std::max<uint32_t>(new_next_rowset_id, new_id + std::max(1L, rowset_meta_pb.num_segments()));
            rowset_meta->set_rowset_seg_id(new_id);
            RowsetSharedPtr* rowset = &new_rowsets[new_id];
            RETURN_IF_ERROR(RowsetFactory::create_rowset(&_tablet.tablet_schema(), _tablet.schema_hash_path(),
                                                         rowset_meta, rowset));
            VLOG(2) << "add a new rowset " << tablet_id << "@" << new_id << "@" << rowset_meta->rowset_id();
        }

        for (auto& [rssid, rowset] : _rowsets) {
            VLOG(2) << "mark rowset " << tablet_id << "@" << rssid << "@" << rowset->rowset_id() << " as unused";
            (void)_unused_rowsets.blocking_put(std::move(rowset));
        }
        STLClearObject(&_edit_version_infos);
        STLClearObject(&_rowsets);
        STLClearObject(&_rowset_stats);

        _apply_version_idx = 0;
        _rowsets = std::move(new_rowsets);

        auto& new_version = _edit_version_infos.emplace_back(std::make_unique<EditVersionInfo>());
        new_version->version = EditVersion(snapshot_meta.snapshot_version(), 0);
        new_version->creation_time = time(nullptr);
        new_version->rowsets.reserve(_rowsets.size());
        for (const auto& [rid, rowset] : _rowsets) {
            new_version->rowsets.emplace_back(rid);
        }
        DCHECK_EQ(1, _edit_version_infos.size());

        WriteBatch wb;
        CHECK_FAIL(TabletMetaManager::clear_log(data_store, &wb, tablet_id));
        for (const auto& [rssid, delvec] : snapshot_meta.delete_vectors()) {
            auto id = rssid + _next_rowset_id;
            CHECK_FAIL(TabletMetaManager::put_del_vector(data_store, &wb, tablet_id, id, delvec));
        }
        // clear dcg before recover from snapshot meta. Otherwise it will fail in some case.
        RETURN_IF_ERROR(TabletMetaManager::clear_delta_column_group(data_store, &wb, tablet_id));
        for (const auto& [rssid, dcglist] : snapshot_meta.delta_column_groups()) {
            for (const auto& dcg : dcglist) {
                const std::vector<std::string> dcg_files = dcg->column_files(_tablet.schema_hash_path());
                for (const auto& dcg_file : dcg_files) {
                    auto st = FileSystem::Default()->path_exists(dcg_file);
                    if (!st.ok()) {
                        return Status::InternalError("delta column file: " + dcg_file +
                                                     " does not exist: " + st.get_error_msg());
                    }
                }
            }
            auto id = rssid + _next_rowset_id;
            CHECK_FAIL(TabletMetaManager::put_delta_column_group(data_store, &wb, tablet_id, id, dcglist));
        }
        for (const auto& [rid, rowset] : _rowsets) {
            RowsetMetaPB meta_pb = rowset->rowset_meta()->to_rowset_pb();
            CHECK_FAIL(TabletMetaManager::put_rowset_meta(data_store, &wb, tablet_id, meta_pb));
        }

        _next_rowset_id = new_next_rowset_id;

        _to_updates_pb_unlocked(new_tablet_meta_pb.mutable_updates());
        VLOG(2) << new_tablet_meta_pb.updates().DebugString();
        CHECK_FAIL(TabletMetaManager::put_tablet_meta(data_store, &wb, new_tablet_meta_pb));

        if (auto st = meta_store->write_batch(&wb); !st.ok()) {
            auto msg = strings::Substitute("Fail to put write batch tablet:$0 $1", tablet_id, st.to_string());
            LOG(ERROR) << msg;
            _set_error(msg);
            return Status::InternalError("fail to put write batch");
        }

        for (const auto& [rid, rowset] : _rowsets) {
            auto stats = std::make_unique<RowsetStats>();
            stats->num_segments = rowset->num_segments();
            stats->num_rows = rowset->num_rows();
            stats->byte_size = rowset->data_disk_size();
            stats->num_dels = _get_rowset_num_deletes(*rowset);
            stats->partial_update_by_column = rowset->is_column_mode_partial_update();
            _calc_compaction_score(stats.get());
            _rowset_stats.emplace(rid, std::move(stats));
        }

        l3.unlock();                     // _rowset_stats_lock
        l2.unlock();                     // _rowsets_lock
        _try_commit_pendings_unlocked(); // may acquire |_rowset_stats_lock| and |_rowsets_lock|

        _update_total_stats(_edit_version_infos[_apply_version_idx]->rowsets, nullptr, nullptr);
        _apply_version_changed.notify_all();
        // The function `unload` of index_entry in the following code acquire `_lock` in PrimaryIndex.
        // If there are other thread to do rowset commit, it will load PrimaryIndex first which hold `_lock` in
        // PrimaryIndex and it will acquire `_lock` in TabletUpdates which is `l1` to get applied rowset which will
        // cause dead lock.
        // Actually, unload PrimayIndex doesn't need to hold `_lock` of TabletUpdates, so we can release l1 in advance
        // to avoid dead lock.
        l1.unlock();

        // unload primary index
        auto manager = StorageEngine::instance()->update_manager();
        auto& index_cache = manager->index_cache();
        auto index_entry = index_cache.get_or_create(tablet_id);
        index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
        index_entry->value().unload();
        index_cache.release(index_entry);

        LOG(INFO) << "load full snapshot done " << _debug_string(false);

        return Status::OK();
    } else {
        return Status::InternalError("unknown snapshot type");
    }
#undef CHECK_FAIL
}

void TabletUpdates::_clear_rowset_del_vec_cache(const Rowset& rowset) {
    StorageEngine::instance()->update_manager()->clear_cached_del_vec([&]() {
        std::vector<TabletSegmentId> tsids;
        tsids.reserve(rowset.num_segments());
        for (auto i = 0; i < rowset.num_segments(); i++) {
            tsids.emplace_back(TabletSegmentId{_tablet.tablet_id(), rowset.rowset_meta()->get_rowset_seg_id() + i});
        }
        return tsids;
    }());
}

void TabletUpdates::_clear_rowset_delta_column_group_cache(const Rowset& rowset) {
    StorageEngine::instance()->update_manager()->clear_cached_delta_column_group([&]() {
        std::vector<TabletSegmentId> tsids;
        tsids.reserve(rowset.num_segments());
        for (auto i = 0; i < rowset.num_segments(); i++) {
            tsids.emplace_back(TabletSegmentId{_tablet.tablet_id(), rowset.rowset_meta()->get_rowset_seg_id() + i});
        }
        return tsids;
    }());
}

Status TabletUpdates::clear_meta() {
    std::lock_guard l1(_lock);
    std::lock_guard l2(_rowsets_lock);
    std::lock_guard l3(_rowset_stats_lock);
    // TODO: tablet is already marked to be deleted, so maybe don't need to clear unused rowsets here
    _remove_unused_rowsets();
    if (_unused_rowsets.get_size() != 0) {
        LOG(WARNING) << "_unused_rowsets is not empty, size: " << _unused_rowsets.get_size()
                     << " version_info: " << _debug_version_info(false);
    }

    WriteBatch wb;
    auto data_store = _tablet.data_dir();
    auto meta_store = data_store->get_meta();

    _set_error("clear_meta inprogress"); // Mark this tablet unusable first.

    // Clear permanently stored meta.
    TabletMetaManager::clear_pending_rowset(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::clear_rowset(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::clear_del_vector(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::clear_delta_column_group(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::clear_log(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::clear_persistent_index(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::remove_tablet_meta(data_store, &wb, _tablet.tablet_id(), _tablet.schema_hash());
    RETURN_IF_ERROR(meta_store->write_batch(&wb));

    // Clear cached delete vectors and cached delta column group
    for (auto& [id, rowset] : _rowsets) {
        _clear_rowset_del_vec_cache(*rowset);
        _clear_rowset_delta_column_group_cache(*rowset);
    }
    // Clear cached primary index.
    StorageEngine::instance()->update_manager()->index_cache().remove_by_key(_tablet.tablet_id());
    STLClearObject(&_rowsets);
    STLClearObject(&_rowset_stats);
    // If this get cleared, every other thread that uses variable should recheck it's valid state after acquiring _lock
    STLClearObject(&_edit_version_infos);
    return Status::OK();
}

void TabletUpdates::_update_total_stats(const std::vector<uint32_t>& rowsets, size_t* row_count_before,
                                        size_t* row_count_after) {
    std::lock_guard l(_rowset_stats_lock);
    if (row_count_before != nullptr) {
        *row_count_before = _cur_total_rows - _cur_total_dels;
    }
    size_t nrow = 0;
    size_t ndel = 0;
    for (auto rid : rowsets) {
        auto itr = _rowset_stats.find(rid);
        if (itr != _rowset_stats.end()) {
            nrow += itr->second->num_rows;
            ndel += itr->second->num_dels;
        }
    }
    _cur_total_rows = nrow;
    _cur_total_dels = ndel;
    if (row_count_after != nullptr) {
        *row_count_after = _cur_total_rows - _cur_total_dels;
    }
}

Status GetDeltaColumnContext::prepareGetDeltaColumnContext(std::shared_ptr<Segment> seg, KVStore* kvstore,
                                                           const TabletSegmentId& tsid, int64_t read_version) {
    segment = std::move(seg);
    auto dcg_loader = std::make_unique<LocalDeltaColumnGroupLoader>(kvstore);
    RETURN_IF_ERROR(dcg_loader->load(tsid, read_version, &dcgs));
    return Status::OK();
}

static StatusOr<std::shared_ptr<Segment>> get_dcg_segment(GetDeltaColumnContext& ctx, uint32_t ucid,
                                                          int32_t* col_index) {
    // iterate dcg from new ver to old ver
    for (const auto& dcg : ctx.dcgs) {
        std::pair<int32_t, int32_t> idx = dcg->get_column_idx(ucid);
        if (idx.first >= 0) {
            std::string column_file = dcg->column_files(parent_name(ctx.segment->file_name()))[idx.first];
            if (ctx.dcg_segments.count(column_file) == 0) {
                ASSIGN_OR_RETURN(auto dcg_segment, ctx.segment->new_dcg_segment(*dcg, idx.first));
                ctx.dcg_segments[column_file] = dcg_segment;
            }
            if (col_index != nullptr) {
                *col_index = idx.second;
            }
            return ctx.dcg_segments[column_file];
        }
    }
    // the column not exist in delta column group
    return nullptr;
}

static StatusOr<std::unique_ptr<ColumnIterator>> new_dcg_column_iterator(GetDeltaColumnContext& ctx,
                                                                         const std::shared_ptr<FileSystem>& fs,
                                                                         ColumnIteratorOptions& iter_opts,
                                                                         uint32_t ucid) {
    // build column iter from delta column group
    int32_t col_index = 0;
    ASSIGN_OR_RETURN(auto dcg_segment, get_dcg_segment(ctx, ucid, &col_index));
    if (dcg_segment != nullptr) {
        if (ctx.dcg_read_files.count(dcg_segment->file_name()) == 0) {
            ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file(dcg_segment->file_name()));
            ctx.dcg_read_files[dcg_segment->file_name()] = std::move(read_file);
        }
        iter_opts.read_file = ctx.dcg_read_files[dcg_segment->file_name()].get();
        return dcg_segment->new_column_iterator(col_index);
    }
    return nullptr;
}

Status TabletUpdates::get_column_values(const std::vector<uint32_t>& column_ids, int64_t read_version,
                                        bool with_default, std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                                        vector<std::unique_ptr<Column>>* columns, void* state) {
    std::vector<uint32_t> unique_column_ids;
    for (unsigned int column_id : column_ids) {
        const TabletColumn& tablet_column = _tablet.tablet_schema().column(column_id);
        unique_column_ids.push_back(tablet_column.unique_id());
    }

    std::map<uint32_t, RowsetSharedPtr> rssid_to_rowsets;
    {
        std::lock_guard<std::mutex> l(_rowsets_lock);
        for (const auto& rowset : _rowsets) {
            rssid_to_rowsets.insert(rowset);
        }
    }
    if (rssid_to_rowsets.empty() && !rowids_by_rssid.empty()) {
        std::string msg =
                strings::Substitute("tablet deleted when call get_column_values() tablet:", _tablet.tablet_id());
        LOG(WARNING) << msg;
        return Status::InternalError(msg);
    }
    if (with_default && state == nullptr) {
        for (auto i = 0; i < column_ids.size(); ++i) {
            const TabletColumn& tablet_column = _tablet.tablet_schema().column(column_ids[i]);
            if (tablet_column.has_default_value()) {
                const TypeInfoPtr& type_info = get_type_info(tablet_column);
                std::unique_ptr<DefaultValueColumnIterator> default_value_iter =
                        std::make_unique<DefaultValueColumnIterator>(
                                tablet_column.has_default_value(), tablet_column.default_value(),
                                tablet_column.is_nullable(), type_info, tablet_column.length(), 1);
                ColumnIteratorOptions iter_opts;
                RETURN_IF_ERROR(default_value_iter->init(iter_opts));
                default_value_iter->fetch_values_by_rowid(nullptr, 1, (*columns)[i].get());
            } else {
                (*columns)[i]->append_default();
            }
        }
    }
    std::shared_ptr<FileSystem> fs;
    for (const auto& [rssid, rowids] : rowids_by_rssid) {
        auto iter = rssid_to_rowsets.upper_bound(rssid);
        --iter;
        const auto& rowset = iter->second.get();
        if (!(rowset->rowset_meta()->get_rowset_seg_id() <= rssid &&
              rssid < rowset->rowset_meta()->get_rowset_seg_id() + rowset->num_segments())) {
            std::string msg = strings::Substitute("illegal rssid: $0, should in [$1, $2)", rssid,
                                                  rowset->rowset_meta()->get_rowset_seg_id(),
                                                  rowset->rowset_meta()->get_rowset_seg_id() + rowset->num_segments());
            LOG(ERROR) << msg;
            _set_error(msg);
            return Status::InternalError(msg);
        }
        // REQUIRE: all rowsets in this tablet have the same path prefix, i.e, can share the same fs
        if (fs == nullptr) {
            ASSIGN_OR_RETURN(fs, FileSystem::CreateSharedFromString(rowset->rowset_path()));
        }
        std::string seg_path =
                Rowset::segment_file_path(rowset->rowset_path(), rowset->rowset_id(), rssid - iter->first);
        auto segment = Segment::open(fs, seg_path, rssid - iter->first, &rowset->schema());
        if (!segment.ok()) {
            LOG(WARNING) << "Fail to open " << seg_path << ": " << segment.status();
            return segment.status();
        }
        if ((*segment)->num_rows() == 0) {
            continue;
        }
        GetDeltaColumnContext ctx;
        RETURN_IF_ERROR(ctx.prepareGetDeltaColumnContext((*segment), _tablet.data_dir()->get_meta(),
                                                         TabletSegmentId(_tablet.tablet_id(), rssid), read_version));
        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file((*segment)->file_name()));
        for (auto i = 0; i < column_ids.size(); ++i) {
            // try to build iterator from delta column file first
            ASSIGN_OR_RETURN(auto col_iter, new_dcg_column_iterator(ctx, fs, iter_opts, unique_column_ids[i]));
            if (col_iter == nullptr) {
                // not found in delta column file, build iterator from main segment
                ASSIGN_OR_RETURN(col_iter, (*segment)->new_column_iterator(column_ids[i]));
                iter_opts.read_file = read_file.get();
            }
            RETURN_IF_ERROR(col_iter->init(iter_opts));
            RETURN_IF_ERROR(col_iter->fetch_values_by_rowid(rowids.data(), rowids.size(), (*columns)[i].get()));
        }
    }
    if (state != nullptr && with_default) {
        auto* auto_increment_state = (AutoIncrementPartialUpdateState*)state;
        Rowset* rowset = auto_increment_state->rowset;
        uint32_t id = auto_increment_state->id;
        const std::vector<uint32_t>& rowids = auto_increment_state->rowids;
        uint32_t segment_id = auto_increment_state->segment_id;
        uint32_t rssid = rowset->rowset_meta()->get_rowset_seg_id() + segment_id;

        std::string seg_path = Rowset::segment_file_path(rowset->rowset_path(), rowset->rowset_id(), segment_id);
        ASSIGN_OR_RETURN(auto fs, FileSystem::CreateSharedFromString(seg_path));
        auto segment = Segment::open(fs, seg_path, segment_id, auto_increment_state->schema);
        if (!segment.ok()) {
            LOG(WARNING) << "Fail to open " << seg_path << ": " << segment.status();
            return segment.status();
        }
        if ((*segment)->num_rows() == 0) {
            return Status::OK();
        }
        GetDeltaColumnContext ctx;
        RETURN_IF_ERROR(ctx.prepareGetDeltaColumnContext((*segment), _tablet.data_dir()->get_meta(),
                                                         TabletSegmentId(_tablet.tablet_id(), rssid), read_version));
        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        ASSIGN_OR_RETURN(auto read_file, fs->new_random_access_file((*segment)->file_name()));
        for (auto i = 0; i < column_ids.size(); ++i) {
            // try to build iterator from delta column file first
            ASSIGN_OR_RETURN(auto col_iter, new_dcg_column_iterator(ctx, fs, iter_opts, unique_column_ids[i]));
            if (col_iter == nullptr) {
                // not found in delta column file, build iterator from main segment
                ASSIGN_OR_RETURN(col_iter, (*segment)->new_column_iterator(id));
                iter_opts.read_file = read_file.get();
            }
            RETURN_IF_ERROR(col_iter->init(iter_opts));
            RETURN_IF_ERROR(col_iter->fetch_values_by_rowid(rowids.data(), rowids.size(), (*columns)[i].get()));
        }
    }
    return Status::OK();
}

Status TabletUpdates::get_rss_rowids_by_pk(Tablet* tablet, const Column& keys, EditVersion* read_version,
                                           std::vector<uint64_t>* rss_rowids, int64_t timeout_ms) {
    if (timeout_ms <= 0) {
        _index_lock.lock();
    } else {
        if (!_index_lock.try_lock_for(std::chrono::milliseconds(timeout_ms))) {
            return Status::TimedOut("get_rss_rowids_by_pk try lock timeout");
        }
    }
    auto st = get_rss_rowids_by_pk_unlock(tablet, keys, read_version, rss_rowids);
    _index_lock.unlock();
    return st;
}

Status TabletUpdates::get_rss_rowids_by_pk_unlock(Tablet* tablet, const Column& keys, EditVersion* read_version,
                                                  std::vector<uint64_t>* rss_rowids) {
    if (read_version != nullptr) {
        // get next_rowset_id and read_version to identify conflict
        std::lock_guard wl(_lock);
        if (_edit_version_infos.empty()) {
            string msg = strings::Substitute("tablet deleted when get_rss_rowids_by_pk tablet:$0", _tablet.tablet_id());
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        *read_version = _edit_version_infos[_apply_version_idx]->version;
    }
    auto manager = StorageEngine::instance()->update_manager();
    auto index_entry = manager->index_cache().get_or_create(tablet->tablet_id());
    index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    bool enable_persistent_index = tablet->get_enable_persistent_index();
    auto& index = index_entry->value();
    auto st = index.load(tablet);
    manager->index_cache().update_object_size(index_entry, index.memory_usage());
    if (!st.ok()) {
        manager->index_cache().remove(index_entry);
        std::string msg = strings::Substitute("get_rss_rowids_by_pk error: load primary index failed: $0 $1",
                                              st.get_error_msg(), debug_string());
        LOG(ERROR) << msg;
        _set_error(msg);
        return Status::InternalError(msg);
    }

    index.get(keys, rss_rowids);

    // if `enable_persistent_index` of tablet is change(maybe changed by alter table)
    // we should try to remove the index_entry from cache
    // Otherwise index may be used for later commits, keep in cache
    if (enable_persistent_index ^ tablet->get_enable_persistent_index()) {
        manager->index_cache().remove(index_entry);
    } else {
        manager->index_cache().release(index_entry);
    }
    return Status::OK();
}

Status TabletUpdates::get_missing_version_ranges(std::vector<int64_t>& missing_version_ranges) {
    // usually, version ranges will have no hole or 1 hole(3 elements in ranges array)
    missing_version_ranges.reserve(3);
    std::lock_guard rl(_lock);
    if (_edit_version_infos.empty()) {
        return Status::InternalError("get_missing_version_ranges:empty edit_version_info");
    }
    int64_t last = _edit_version_infos.back()->version.major();
    for (auto& itr : _pending_commits) {
        int64_t cur = itr.first;
        if (last + 1 < cur) {
            missing_version_ranges.push_back(last + 1);
            missing_version_ranges.push_back(cur - 1);
        }
        last = cur;
    }
    missing_version_ranges.push_back(last + 1);
    return Status::OK();
}

Status TabletUpdates::get_rowsets_for_incremental_snapshot(const std::vector<int64_t>& missing_version_ranges,
                                                           std::vector<RowsetSharedPtr>& rowsets) {
    // TODO: also find rowsets in pending_rowsets
    vector<int64_t> versions;
    vector<uint32_t> rowsetids;
    {
        std::lock_guard lg(_lock);
        if (_edit_version_infos.empty()) {
            string msg = strings::Substitute("tablet deleted when get_rowsets_for_incremental_snapshot tablet:$0",
                                             _tablet.tablet_id());
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        int64_t vmax = _edit_version_infos.back()->version.major();
        for (size_t i = 0; i + 1 < missing_version_ranges.size(); i += 2) {
            for (size_t v = missing_version_ranges[i]; v <= missing_version_ranges[i + 1]; v++) {
                if (v <= vmax) {
                    versions.push_back(v);
                } else {
                    break;
                }
            }
        }
        int64_t vcur = missing_version_ranges.back();
        while (vcur <= vmax) {
            versions.push_back(vcur++);
        }
        if (versions.empty()) {
            string msg = strings::Substitute("get_rowsets_for_snapshot: no version to clone $0 request_version:$1,",
                                             _debug_version_info(false), missing_version_ranges.back());
            LOG(INFO) << msg;
            return Status::NotFound(msg);
        }
        size_t num_rowset_full_clone = _edit_version_infos.back()->rowsets.size();
        // TODO: make better estimates about incremental / full clone costs
        // currently only consider number of rowsets, should also consider number of actual files and data size to
        // reflect the actual cost
        if (versions.size() >= config::tablet_max_versions || versions.size() >= num_rowset_full_clone * 20) {
            string msg = strings::Substitute(
                    "get_rowsets_for_snapshot: too many rowsets for incremental clone "
                    "#rowset:$0 #rowset_for_full_clone:$1 switch to full clone $2",
                    versions.size(), num_rowset_full_clone, _debug_version_info(false));
            LOG(INFO) << msg;
            return Status::OK();
        }
        rowsetids.reserve(versions.size());
        // compare two lists to find matching versions and record rowsetid
        size_t versions_index = 0;
        size_t edit_versions_index = 0;
        while (versions_index < versions.size() && edit_versions_index < _edit_version_infos.size()) {
            auto& edit_version_info = _edit_version_infos[edit_versions_index];
            if (edit_version_info->deltas.empty()) {
                edit_versions_index++;
                continue;
            }
            auto edit_version = edit_version_info->version.major();
            if (edit_version == versions[versions_index]) {
                DCHECK_EQ(1, edit_version_info->deltas.size());
                rowsetids.emplace_back(edit_version_info->deltas[0]);
                versions_index++;
                edit_versions_index++;
            } else if (edit_version > versions[versions_index]) {
                // some version is missing, maybe already GCed, switch to full_snapshot
                LOG(WARNING) << strings::Substitute("get_rowsets_for_incremental_snapshot: version $0 not found $1",
                                                    versions[versions_index], _debug_version_info(false));
                return Status::OK();
            } else {
                edit_versions_index++;
            }
        }
    }
    // get rowset from rowsetid
    {
        std::lock_guard lg2(_rowsets_lock);
        for (auto rid : rowsetids) {
            auto itr = _rowsets.find(rid);
            if (itr != _rowsets.end()) {
                rowsets.push_back(itr->second);
            } else {
                LOG(ERROR) << strings::Substitute(
                        "get_rowsets_for_incremental_snapshot: rowset not found tablet:$0 rowsetid:$1",
                        _tablet.tablet_id(), rid);
            }
        }
    }
    LOG(INFO) << strings::Substitute("get_rowsets_for_incremental_snapshot $0 missing_ranges:$1 return:$2",
                                     _debug_version_info(true), JoinInts(missing_version_ranges, ","),
                                     JoinInts(versions, ","));
    return Status::OK();
}

void TabletUpdates::to_rowset_meta_pb(const std::vector<RowsetMetaSharedPtr>& rowset_metas,
                                      std::vector<RowsetMetaPB>& rowset_metas_pb) {
    std::lock_guard wl(_lock);
    rowset_metas_pb.reserve(rowset_metas.size());
    for (const auto& rowset_meta : rowset_metas) {
        RowsetMetaPB& meta_pb = rowset_metas_pb.emplace_back();
        rowset_meta->to_rowset_pb(&meta_pb);
    }
}

std::vector<std::string> TabletUpdates::get_version_list() const {
    std::lock_guard wl(_lock);
    std::vector<std::string> version_list;
    for (auto& edit_version_info : _edit_version_infos) {
        version_list.emplace_back(edit_version_info->version.to_string());
    }
    return version_list;
}

std::shared_ptr<EditVersionInfo> TabletUpdates::get_edit_version(const string& version) const {
    std::lock_guard wl(_lock);
    for (auto& edit_version_info : _edit_version_infos) {
        if (edit_version_info->version.to_string() == version) {
            return std::make_shared<EditVersionInfo>(*edit_version_info);
        }
    }
    return nullptr;
}

std::shared_ptr<std::unordered_map<uint32_t, RowsetSharedPtr>> TabletUpdates::get_rowset_map() const {
    std::lock_guard lg(_rowsets_lock);
    return std::make_shared<std::unordered_map<uint32_t, RowsetSharedPtr>>(_rowsets);
}

Status TabletUpdates::get_rowset_and_segment_idx_by_rssid(uint32_t rssid, RowsetSharedPtr* rowset,
                                                          uint32_t* segment_idx) {
    std::lock_guard<std::mutex> l(_rowsets_lock);
    for (const auto& rssid_rowset_pair : _rowsets) {
        if (rssid >= rssid_rowset_pair.first &&
            rssid < rssid_rowset_pair.first + rssid_rowset_pair.second->num_segments()) {
            *rowset = rssid_rowset_pair.second;
            *segment_idx = rssid - rssid_rowset_pair.first;
            return Status::OK();
        }
    }
    return Status::NotFound(strings::Substitute("rowset for rssid $0 not found", rssid));
}

} // namespace starrocks
