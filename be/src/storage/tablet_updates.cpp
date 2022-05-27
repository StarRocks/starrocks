// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/tablet_updates.h"

#include <algorithm>
#include <ctime>
#include <memory>

#include "column/datum.h"
#include "common/status.h"
#include "gen_cpp/MasterService_types.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/stl_util.h"
#include "gutil/strings/join.h"
#include "gutil/strings/substitute.h"
#include "rocksdb/write_batch.h"
#include "runtime/current_thread.h"
#include "runtime/exec_env.h"
#include "storage/del_vector.h"
#include "storage/primary_key_encoder.h"
#include "storage/rowset/default_value_column_iterator.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/rowset/rowset_writer.h"
#include "storage/rowset/rowset_writer_context.h"
#include "storage/rowset/vectorized/rowset_options.h"
#include "storage/rowset/vectorized/segment_iterator.h"
#include "storage/rowset/vectorized/segment_options.h"
#include "storage/rowset_update_state.h"
#include "storage/snapshot_meta.h"
#include "storage/storage_engine.h"
#include "storage/tablet.h"
#include "storage/tablet_meta_manager.h"
#include "storage/types.h"
#include "storage/update_compaction_state.h"
#include "storage/update_manager.h"
#include "storage/vectorized/chunk_helper.h"
#include "storage/vectorized/chunk_iterator.h"
#include "storage/vectorized/compaction.h"
#include "storage/vectorized/rowset_merger.h"
#include "storage/vectorized/schema_change.h"
#include "storage/wrapper_field.h"
#include "util/defer_op.h"
#include "util/pretty_printer.h"
#include "util/scoped_cleanup.h"

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
        _set_error();
        string msg = "updatable tablet do not have updates meta";
        LOG(ERROR) << msg;
        return Status::InternalError(msg);
    }
    return _load_from_pb(*updates);
}

Status TabletUpdates::_load_from_pb(const TabletUpdatesPB& tablet_updates_pb) {
    std::unique_lock l1(_lock);
    std::unique_lock l2(_rowsets_lock);
    const auto& edit_version_meta_pbs = tablet_updates_pb.versions();
    if (edit_version_meta_pbs.empty()) {
        _set_error();
        string msg = "tablet_updates_pb.edit_version_meta_pbs should have at least 1 version";
        LOG(ERROR) << msg;
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

    // Load pending rowsets
    _pending_commits.clear();
    RETURN_IF_ERROR(TabletMetaManager::pending_rowset_iterate(
            _tablet.data_dir(), _tablet.tablet_id(),
            [&](int64_t version, const std::string_view& rowset_meta_data) -> bool {
                RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
                CHECK(rowset_meta->init(rowset_meta_data)) << "Corrupted rowset meta";
                RowsetSharedPtr rowset;
                st = RowsetFactory::create_rowset(&_tablet.tablet_schema(), _tablet.schema_hash_path(), rowset_meta,
                                                  &rowset);
                if (st.ok()) {
                    _pending_commits.emplace(version, rowset);
                } else {
                    LOG(WARNING) << "Fail to create rowset from pending rowset meta. rowset="
                                 << rowset_meta->rowset_id() << " type=" << rowset_meta->rowset_type()
                                 << " state=" << rowset_meta->rowset_state();
                }
                return true;
            }));

    std::set<uint32_t> all_rowsets;
    std::set<uint32_t> active_rowsets;
    std::set<uint32_t> unapplied_rowsets;
    std::vector<uint32_t> unused_rowsets;

    // Load all rowsets of this tablet into memory.
    // NOTE: This may change in a near future, e.g, manage rowsets in a separate module and load
    // them on demand.
    RETURN_IF_ERROR(TabletMetaManager::rowset_iterate(
            _tablet.data_dir(), _tablet.tablet_id(), [&](const RowsetMetaSharedPtr& rowset_meta) -> bool {
                RowsetSharedPtr rowset;
                st = RowsetFactory::create_rowset(&_tablet.tablet_schema(), _tablet.schema_hash_path(), rowset_meta,
                                                  &rowset);
                if (st.ok()) {
                    _rowsets[rowset_meta->get_rowset_seg_id()] = std::move(rowset);
                } else {
                    LOG(WARNING) << "Fail to create rowset from rowset meta. rowset=" << rowset_meta->rowset_id()
                                 << " type=" << rowset_meta->rowset_type() << " state=" << rowset_meta->rowset_state();
                }
                all_rowsets.insert(rowset_meta->get_rowset_seg_id());
                return true;
            }));

    // Find unused rowsets.
    for (size_t i = 0; i < _edit_version_infos.size(); i++) {
        auto& rs = _edit_version_infos[i]->rowsets;
        for (auto rid : rs) {
            bool inserted = active_rowsets.insert(rid).second;
            if (i > _apply_version_idx && inserted) {
                // it's a newly added rowset which have not been applied yet
                unapplied_rowsets.insert(rid);
            }
        }
    }
    DCHECK_LE(active_rowsets.size(), all_rowsets.size()) << " tabletid:" << _tablet.tablet_id();

    std::set_difference(all_rowsets.begin(), all_rowsets.end(), active_rowsets.begin(), active_rowsets.end(),
                        std::back_inserter(unused_rowsets));
    for (uint32_t id : unused_rowsets) {
        auto iter = _rowsets.find(id);
        DCHECK(iter != _rowsets.end());
        _unused_rowsets.blocking_put(std::move(iter->second));
        _rowsets.erase(iter);
        all_rowsets.erase(id);
    }

    if (active_rowsets.size() > all_rowsets.size()) {
        std::vector<uint32_t> missing_rowsets;
        std::set_difference(active_rowsets.begin(), active_rowsets.end(), all_rowsets.begin(), all_rowsets.end(),
                            std::back_inserter(missing_rowsets));
        LOG(ERROR) << "tablet init missing rowset, tablet:" << _tablet.tablet_id()
                   << " all:" << JoinInts(all_rowsets, ",") << " active:" << JoinInts(active_rowsets, ",")
                   << " missing:" << JoinInts(missing_rowsets, ",");
        _set_error();
        return Status::OK();
    }

    // Load delete vectors and update RowsetStats.
    // TODO: save num_dels in rowset meta.
    for (auto& [rsid, rowset] : _rowsets) {
        auto stats = std::make_unique<RowsetStats>();
        stats->num_segments = rowset->num_segments();
        stats->num_rows = rowset->num_rows();
        stats->byte_size = rowset->data_disk_size();
        stats->num_dels = 0;
        if (unapplied_rowsets.find(rsid) == unapplied_rowsets.end()) {
            // rowset applied, must have delvec
            for (int i = 0; i < rowset->num_segments(); i++) {
                DelVector delvec;
                int64_t dummy;
                auto st = TabletMetaManager::get_del_vector(_tablet.data_dir()->get_meta(), _tablet.tablet_id(),
                                                            rsid + i, INT64_MAX, &delvec, &dummy);
                if (!st.ok()) {
                    LOG(ERROR) << "_load_from_pb get_del_vector failed: " << st;
                    _set_error();
                    return Status::OK();
                }
                stats->num_dels += delvec.cardinality();
            }
            DCHECK_LE(stats->num_dels, stats->num_rows) << " tabletid:" << _tablet.tablet_id() << " rowset:" << rsid;
        }
        _calc_compaction_score(stats.get());
        std::lock_guard lg(_rowset_stats_lock);
        _rowset_stats.emplace(rsid, std::move(stats));
    }
    l2.unlock(); // _rowsets_lock
    _update_total_stats(_edit_version_infos[_apply_version_idx]->rowsets);
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
        string msg = Substitute("get_rowset_total_stats() some rowset stats not found tablet:$0 rowsets:$1",
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
    LOG(ERROR) << "illegal state, apply version not found in versions";
    _set_error();
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

Status TabletUpdates::_get_apply_version_and_rowsets(int64_t* version, std::vector<RowsetSharedPtr>* rowsets,
                                                     std::vector<uint32_t>* rowset_ids) {
    std::lock_guard rl(_lock);
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
                    Substitute("get_apply_version_and_rowsets rowset not found: version:$0 rowset:$1 $2", version, rsid,
                               _debug_string(false, true)));
        }
    }
    rowset_ids->assign(edit_version_info->rowsets.begin(), edit_version_info->rowsets.end());
    *version = edit_version_info->version.major();
    return Status::OK();
}

Status TabletUpdates::rowset_commit(int64_t version, const RowsetSharedPtr& rowset) {
    if (_error) {
        return Status::InternalError(Substitute("rowset_commit failed, tablet updates is in error state: tablet:$0 $1",
                                                _tablet.tablet_id(), _error_msg));
    }
    Status st;
    {
        std::lock_guard wl(_lock);
        if (version <= _edit_version_infos.back()->version.major()) {
            LOG(WARNING) << "ignored already committed version " << version << " of tablet " << _tablet.tablet_id()
                         << " txn:" << rowset->txn_id();
            _ignore_rowset_commit(version, rowset);
            return Status::OK();
        } else if (version > _edit_version_infos.back()->version.major() + 1) {
            if (_pending_commits.size() >= config::tablet_max_pending_versions) {
                // there must be something wrong, return error rather than accepting more commits
                string msg = Substitute(
                        "rowset commit failed too many pending rowsets tablet:$0 version:$1 txn:$2 #pending:$3",
                        _tablet.tablet_id(), version, rowset->txn_id(), _pending_commits.size());
                LOG(WARNING) << msg;
                return Status::InternalError(msg);
            }
            if (!_pending_commits.emplace(version, rowset).second) {
                LOG(WARNING) << "ignore add rowset to pending commits, same version already exists version:" << version
                             << " txn:" << rowset->txn_id() << " " << _debug_string(false, false);
                _ignore_rowset_commit(version, rowset);
            } else {
                st = TabletMetaManager::pending_rowset_commit(
                        _tablet.data_dir(), _tablet.tablet_id(), version, rowset->rowset_meta()->get_meta_pb(),
                        RowsetMetaManager::get_rowset_meta_key(_tablet.tablet_uid(), rowset->rowset_id()));
                if (!st.ok()) {
                    LOG(WARNING) << "add rowset to pending commits failed tablet:" << _tablet.tablet_id()
                                 << " version:" << version << " txn:" << rowset->txn_id() << " " << st << " "
                                 << _debug_string(false, true);
                    return st;
                }
                LOG(INFO) << "add rowset to pending commits tablet:" << _tablet.tablet_id() << " version:" << version
                          << " txn:" << rowset->txn_id() << " #pending:" << _pending_commits.size();
            }
            return Status::OK();
        }
        st = _rowset_commit_unlocked(version, rowset);
        if (st.ok()) {
            LOG(INFO) << "commit rowset tablet:" << _tablet.tablet_id() << " version:" << version
                      << " txn:" << rowset->txn_id() << " " << rowset->rowset_id().to_string()
                      << " rowset:" << rowset->rowset_meta()->get_rowset_seg_id() << " #seg:" << rowset->num_segments()
                      << " #row:" << rowset->num_rows()
                      << " size:" << PrettyPrinter::print(rowset->data_disk_size(), TUnit::BYTES)
                      << " #pending:" << _pending_commits.size();
            _try_commit_pendings_unlocked();
            _check_for_apply();
        }
    }
    if (!st.ok()) {
        LOG(WARNING) << "rowset commit failed tablet:" << _tablet.tablet_id() << " version:" << version
                     << " txn:" << rowset->txn_id() << " pending:" << _pending_commits.size() << " msg:" << st;
    }
    return st;
}

Status TabletUpdates::_rowset_commit_unlocked(int64_t version, const RowsetSharedPtr& rowset) {
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
    auto st = TabletMetaManager::rowset_commit(
            _tablet.data_dir(), _tablet.tablet_id(), _next_log_id, &edit, rowset->rowset_meta()->get_meta_pb(),
            RowsetMetaManager::get_rowset_meta_key(_tablet.tablet_uid(), rowset->rowset_id()));
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
            LOG(ERROR) << Substitute("creation_time decreased tablet:$0 $1:$2 > $3:$4", _tablet.tablet_id(),
                                     last2->version.to_string(), last2->creation_time, last1->version.to_string(),
                                     last1->creation_time);
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
                             << " txn:" << itr->second->txn_id() << " #pending:" << _pending_commits.size();
                _ignore_rowset_commit(version, itr->second);
                auto st = TabletMetaManager::delete_pending_rowset(_tablet.data_dir(), _tablet.tablet_id(), version);
                LOG_IF(WARNING, !st.ok())
                        << "Failed to delete_pending_rowset tablet:" << _tablet.tablet_id() << " version:" << version
                        << " txn:" << itr->second->txn_id() << " rowset: " << itr->second->rowset_id().to_string();
                itr = _pending_commits.erase(itr);
            } else if (version == current_version + 1) {
                auto& rowset = itr->second;
                auto st = _rowset_commit_unlocked(version, rowset);
                if (!st.ok()) {
                    LOG(ERROR) << "commit rowset (pending) failed tablet: " << _tablet.tablet_id()
                               << " version:" << version << " txn:" << rowset->txn_id()
                               << " rowset:" << rowset->rowset_meta()->get_rowset_seg_id()
                               << " #seg:" << rowset->num_segments() << " #row:" << rowset->num_rows()
                               << " size:" << PrettyPrinter::print(rowset->data_disk_size(), TUnit::BYTES)
                               << " #pending:" << _pending_commits.size() << " " << st.to_string();
                    return;
                }
                LOG(INFO) << "commit rowset (pending) tablet:" << _tablet.tablet_id() << " version:" << version
                          << " txn:" << rowset->txn_id() << " rowset:" << rowset->rowset_meta()->get_rowset_seg_id()
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
                              << " txn:" << rowset->txn_id() << " rowset: " << rowset->rowset_id().to_string();
}

Status TabletUpdates::save_meta() {
    TabletMetaPB metapb;
    // No need to acquire the meta lock?
    _tablet._tablet_meta->to_meta_pb(&metapb);
    return TabletMetaManager::save(_tablet.data_dir(), _tablet.tablet_id(), _tablet.schema_hash(), metapb);
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
        _set_error();
        LOG(ERROR) << "submit apply task failed: " << st << _debug_string(false, false);
    }
}

void TabletUpdates::do_apply() {
    // only 1 thread at max is running this method
    bool first = true;
    while (!_apply_stopped) {
        const EditVersionInfo* version_info_apply = nullptr;
        {
            std::lock_guard rl(_lock);
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
            LOG(ERROR) << "bad EditVersionInfo tablet:" << _tablet.tablet_id();
            _set_error();
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

void TabletUpdates::_apply_rowset_commit(const EditVersionInfo& version_info) {
    // NOTE: after commit, apply must success or fatal crash
    int64_t t_start = MonotonicMillis();
    auto tablet_id = _tablet.tablet_id();
    uint32_t rowset_id = version_info.deltas[0];
    auto& version = version_info.version;
    VLOG(1) << "apply_rowset_commit start tablet:" << tablet_id << " version:" << version.to_string()
            << " rowset:" << rowset_id;
    RowsetSharedPtr rowset = _get_rowset(rowset_id);
    auto manager = StorageEngine::instance()->update_manager();

    // 1. load upserts/deletes in rowset
    auto state_entry = manager->update_state_cache().get_or_create(
            Substitute("$0_$1", tablet_id, rowset->rowset_id().to_string()));
    state_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    auto& state = state_entry->value();
    auto st = state.load(&_tablet, rowset.get());
    manager->update_state_cache().update_object_size(state_entry, state.memory_usage());
    if (!st.ok()) {
        LOG(ERROR) << "_apply_rowset_commit error: load rowset update state failed: " << st << " " << debug_string();
        manager->update_state_cache().remove(state_entry);
        _set_error();
        return;
    }

    std::lock_guard lg(_index_lock);
    // 2. load index
    auto index_entry = manager->index_cache().get_or_create(tablet_id);
    index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    auto& index = index_entry->value();
    st = index.load(&_tablet);
    manager->index_cache().update_object_size(index_entry, index.memory_usage());
    if (!st.ok()) {
        LOG(ERROR) << "_apply_rowset_commit error: load primary index failed: " << st << " " << debug_string();
        manager->index_cache().remove(index_entry);
        _set_error();
        return;
    }

    int64_t t_load = MonotonicMillis();
    st = state.apply(&_tablet, rowset.get(), rowset_id, index);
    if (!st.ok()) {
        LOG(ERROR) << "_apply_rowset_commit error: apply rowset update state failed: " << st << " " << debug_string();
        manager->update_state_cache().remove(state_entry);
        _set_error();
        return;
    }
    int64_t t_apply = MonotonicMillis();

    // 3. generate delvec
    // add initial empty delvec for new segments
    PrimaryIndex::DeletesMap new_deletes;
    size_t delete_op = 0;
    for (uint32_t i = 0; i < rowset->num_segments(); i++) {
        new_deletes[rowset_id + i] = {};
    }
    auto& upserts = state.upserts();
    for (uint32_t i = 0; i < upserts.size(); i++) {
        if (upserts[i] != nullptr) {
            index.upsert(rowset_id + i, 0, *upserts[i], &new_deletes);
            manager->index_cache().update_object_size(index_entry, index.memory_usage());
        }
    }

    for (const auto& one_delete : state.deletes()) {
        delete_op += one_delete->size();
        index.erase(*one_delete, &new_deletes);
    }
    manager->index_cache().update_object_size(index_entry, index.memory_usage());
    // release resource
    // update state only used once, so delete it
    manager->update_state_cache().remove(state_entry);
    // index may be used for later commits, so keep in cache
    manager->index_cache().release(index_entry);
    int64_t t_index = MonotonicMillis();

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
                LOG(ERROR) << "_apply_rowset_commit error: get_latest_del_vec failed: " << st << " " << debug_string();
                _set_error();
                return;
            }
            new_del_vecs[idx].first = rssid;
            old_del_vec->add_dels_as_new_version(new_delete.second, version.major(), &(new_del_vecs[idx].second));
            size_t cur_old = old_del_vec->cardinality();
            size_t cur_add = new_delete.second.size();
            size_t cur_new = new_del_vecs[idx].second->cardinality();
            if (cur_old + cur_add != cur_new) {
                // should not happen, data inconsistent
                LOG(FATAL) << Substitute(
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
            string msg = Substitute("inconsistent rowset_stats, rowset not found tablet=$0 rssid=$1 $2",
                                    _tablet.tablet_id(), rssid);
            DCHECK(false) << msg;
            LOG(ERROR) << msg;
        } else if (rssid >= iter->first + iter->second->num_segments) {
            string msg = Substitute("inconsistent rowset_stats, tablet=$0 rssid=$1 >= $2", _tablet.tablet_id(), rssid,
                                    iter->first + iter->second->num_segments);
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
        // 4. write meta
        st = TabletMetaManager::apply_rowset_commit(_tablet.data_dir(), tablet_id, _next_log_id, version, new_del_vecs);
        if (!st.ok()) {
            LOG(ERROR) << "_apply_rowset_commit error: write meta failed: " << st << " " << _debug_string(false);
            _set_error();
            return;
        }
        // put delvec in cache
        TabletSegmentId tsid;
        tsid.tablet_id = tablet_id;
        for (auto& delvec_pair : new_del_vecs) {
            tsid.segment_id = delvec_pair.first;
            manager->set_cached_del_vec(tsid, delvec_pair.second);
        }
        // 5. apply memory
        _next_log_id++;
        _apply_version_idx++;
        _apply_version_changed.notify_all();
    }
    _update_total_stats(version_info.rowsets);
    int64_t t_write = MonotonicMillis();

    size_t del_percent = _cur_total_rows == 0 ? 0 : (_cur_total_dels * 100) / _cur_total_rows;
    LOG(INFO) << "apply_rowset_commit finish. tablet:" << tablet_id << " version:" << version_info.version.to_string()
              << " txn:" << rowset->txn_id() << " total del/row:" << _cur_total_dels << "/" << _cur_total_rows << " "
              << del_percent << "% rowset:" << rowset_id << " #seg:" << rowset->num_segments()
              << " #op(upsert:" << rowset->num_rows() << " del:" << delete_op << ") #del:" << old_total_del << "+"
              << new_del << "=" << total_del << " #dv:" << ndelvec << " duration:" << t_write - t_start << "ms"
              << Substitute("($0/$1/$2/$3/$4)", t_load - t_start, t_apply - t_load, t_index - t_apply,
                            t_delvec - t_index, t_write - t_delvec);
    VLOG(1) << "rowset commit apply " << delvec_change_info << " " << _debug_string(true, true);
}

RowsetSharedPtr TabletUpdates::_get_rowset(uint32_t rowset_id) {
    std::lock_guard<std::mutex> lg(_rowsets_lock);
    auto itr = _rowsets.find(rowset_id);
    if (itr == _rowsets.end()) {
        // TODO: _rowsets will act as a cache in the future
        // need to load rowset from rowsetdb, currently just return null
        return RowsetSharedPtr();
    }
    return itr->second;
}

Status TabletUpdates::_wait_for_version(const EditVersion& version, int64_t timeout_ms) {
    std::unique_lock<std::mutex> ul(_lock);
    if (!(_edit_version_infos[_apply_version_idx]->version < version)) {
        return Status::OK();
    }
    int64_t wait_start = MonotonicMillis();
    while (true) {
        if (!_apply_running) {
            return Status::InternalError(Substitute("wait_for_version version:$0 failed: apply stopped $1",
                                                    version.to_string(), _debug_string(false, true)));
        }
        _apply_version_changed.wait_for(ul, std::chrono::seconds(2));
        if (_error) {
            break;
        }
        int64_t now = MonotonicMillis();
        if (!(_edit_version_infos[_apply_version_idx]->version < version)) {
            if (now - wait_start > 3000) {
                LOG(WARNING) << Substitute("wait_for_version slow($0ms) version:$1 $2", now - wait_start,
                                           version.to_string(), _debug_string(false, true));
            }
            break;
        }
        if (_edit_version_infos.back()->version < version &&
            (_pending_commits.empty() || _pending_commits.rbegin()->first < version.major())) {
            string msg = Substitute("wait_for_version failed version:$0 $1", version.to_string(),
                                    _debug_string(false, true));
            LOG(WARNING) << msg;
            return Status::InternalError(msg);
        }
        if (now - wait_start > timeout_ms) {
            string msg = Substitute("wait_for_version timeout($0ms) version:$1 $2", now - wait_start,
                                    version.to_string(), _debug_string(false, true));
            LOG(WARNING) << msg;
            return Status::TimedOut(msg);
        }
    }
    return Status::OK();
}

StatusOr<std::unique_ptr<CompactionInfo>> TabletUpdates::_get_compaction() {
    std::unique_ptr<CompactionInfo> info = std::make_unique<CompactionInfo>();
    std::lock_guard rl(_lock);
    // 1. start compaction at current apply version
    info->start_version = _edit_version_infos[_apply_version_idx]->version;
    // 2. TODO: select compaction input rowsets
    // currently just select all rowset for demo purpose
    info->inputs = _edit_version_infos[_apply_version_idx]->rowsets;
    return info;
}

Status TabletUpdates::_do_compaction(std::unique_ptr<CompactionInfo>* pinfo, bool wait_apply) {
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
                _set_error();
                string msg =
                        Substitute("_do_compaction rowset $0 should exists $1", info->inputs[i], _debug_string(false));
                LOG(ERROR) << msg;
                return Status::InternalError(msg);
            } else {
                input_rowsets[i] = itr->second;
                input_rowsets_size = input_rowsets[i]->data_disk_size();
                input_row_num = input_rowsets[i]->num_rows();
            }
        }
    }

    uint32_t max_rows_per_segment = vectorized::Compaction::get_segment_max_rows(config::max_segment_file_size,
                                                                                 input_row_num, input_rowsets_size);

    int64_t max_columns_per_group = config::vertical_compaction_max_columns_per_group;
    size_t num_columns = _tablet.num_columns();
    vectorized::CompactionAlgorithm algorithm = vectorized::Compaction::choose_compaction_algorithm(
            num_columns, max_columns_per_group, input_rowsets.size());

    // create rowset writer
    RowsetWriterContext context(kDataFormatV2, config::storage_format_version);
    context.rowset_id = StorageEngine::instance()->next_rowset_id();
    context.tablet_uid = _tablet.tablet_uid();
    context.tablet_id = _tablet.tablet_id();
    context.partition_id = _tablet.partition_id();
    context.tablet_schema_hash = _tablet.schema_hash();
    context.rowset_type = BETA_ROWSET;
    context.rowset_path_prefix = _tablet.schema_hash_path();
    context.tablet_schema = &(_tablet.tablet_schema());
    context.rowset_state = COMMITTED;
    context.segments_overlap = NONOVERLAPPING;
    context.max_rows_per_segment = max_rows_per_segment;
    context.writer_type =
            (algorithm == vectorized::kVertical ? RowsetWriterType::kVertical : RowsetWriterType::kHorizontal);
    std::unique_ptr<RowsetWriter> rowset_writer;
    Status st = RowsetFactory::create_rowset_writer(context, &rowset_writer);
    if (!st.ok()) {
        std::stringstream ss;
        ss << "Fail to create rowset writer err=" << st << " " << debug_string();
        LOG(WARNING) << ss.str();
        return Status::InternalError(ss.str());
    }
    vectorized::MergeConfig cfg;
    cfg.chunk_size = config::vector_chunk_size;
    cfg.algorithm = algorithm;
    RETURN_IF_ERROR(vectorized::compaction_merge_rowsets(_tablet, info->start_version.major(), input_rowsets,
                                                         rowset_writer.get(), cfg));
    auto output_rowset = rowset_writer->build();
    if (!output_rowset.ok()) return output_rowset.status();
    // 4. commit compaction
    EditVersion version;
    RETURN_IF_ERROR(_commit_compaction(pinfo, *output_rowset, &version));
    if (wait_apply) {
        // already committed, so we can only ignore timeout error
        _wait_for_version(version, 120000);
    }
    return Status::OK();
}

Status TabletUpdates::_commit_compaction(std::unique_ptr<CompactionInfo>* pinfo, const RowsetSharedPtr& rowset,
                                         EditVersion* commit_version) {
    _compaction_state = std::make_unique<vectorized::CompactionState>();
    RETURN_IF_ERROR(_compaction_state->load(rowset.get()));
    std::lock_guard wl(_lock);
    EditVersionMetaPB edit;
    auto lastv = _edit_version_infos.back().get();
    auto edit_version_pb = edit.mutable_version();
    edit_version_pb->set_major(lastv->version.major());
    edit_version_pb->set_minor(lastv->version.minor() + 1);
    int64_t creation_time = time(nullptr);
    edit.set_creation_time(creation_time);
    uint32_t rowsetid = _next_rowset_id;
    auto& inputs = (*pinfo)->inputs;
    auto& ors = _edit_version_infos.back()->rowsets;
    for (auto rowset_id : inputs) {
        if (std::find(ors.begin(), ors.end(), rowset_id) == ors.end()) {
            // This may happen after a full clone.
            _compaction_state.reset();
            auto msg = Substitute("compaction input rowset($0) not found $2", rowset_id, _debug_string(false, false));
            LOG(WARNING) << msg;
            return Status::Cancelled(msg);
        }
    }
    CHECK(inputs.size() <= ors.size()) << Substitute("compaction input size($0) > rowset size($1) tablet:$2",
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
    return Status::OK();
}

void TabletUpdates::_apply_compaction_commit(const EditVersionInfo& version_info) {
    // NOTE: after commit, apply must success or fatal crash
    auto info = version_info.compaction.get();
    CHECK(info != nullptr) << "compaction info empty";
    // if compaction_state == null, it must be the case that BE restarted
    // need to rebuild/load state from disk
    if (!_compaction_state) {
        _compaction_state = std::make_unique<vectorized::CompactionState>();
    }
    int64_t t_start = MonotonicMillis();
    auto manager = StorageEngine::instance()->update_manager();
    auto tablet_id = _tablet.tablet_id();
    uint32_t rowset_id = version_info.compaction->output;
    auto& version = version_info.version;
    LOG(INFO) << "apply_compaction_commit start tablet:" << tablet_id << " version:" << version_info.version.to_string()
              << " rowset:" << rowset_id;
    // 1. load index
    auto index_entry = manager->index_cache().get_or_create(tablet_id);
    index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    auto& index = index_entry->value();
    auto st = index.load(&_tablet);
    if (!st.ok()) {
        LOG(ERROR) << "_apply_compaction_commit error: load primary index failed: " << st << " " << debug_string();
        manager->index_cache().remove(index_entry);
        _compaction_state.reset();
        _set_error();
        return;
    }
    if (!(st = _compaction_state->load(_get_rowset(rowset_id).get())).ok()) {
        manager->index_cache().release(index_entry);
        _compaction_state.reset();
        LOG(ERROR) << "_apply_compaction_commit error: load compaction state failed: " << st << " " << debug_string();
        _set_error();
        return;
    }
    int64_t t_load = MonotonicMillis();
    // 2. iterator new rowset's pks, update primary index, generate delvec
    size_t total_deletes = 0;
    size_t total_rows = 0;
    vector<std::pair<uint32_t, DelVectorPtr>> delvecs;
    vector<uint32_t> tmp_deletes;
    for (size_t i = 0; i < _compaction_state->segment_states.size(); i++) {
        auto& sstate = _compaction_state->segment_states[i];
        total_rows += sstate.src_rssids.size();
        uint32_t rssid = rowset_id + i;
        tmp_deletes.clear();
        // replace will not grow hashtable, so don't need to check memory limit
        index.try_replace(rssid, 0, *sstate.pkeys, sstate.src_rssids, &tmp_deletes);
        DelVectorPtr dv = std::make_shared<DelVector>();
        if (tmp_deletes.empty()) {
            dv->init(version.major(), nullptr, 0);
        } else {
            dv->init(version.major(), tmp_deletes.data(), tmp_deletes.size());
            total_deletes += tmp_deletes.size();
        }
        delvecs.emplace_back(rssid, dv);
        // release memory early
        sstate.pkeys.reset();
        sstate.src_rssids.clear();
    }
    // release memory
    _compaction_state.reset();
    // index may be used for later commits, so keep in cache
    manager->index_cache().release(index_entry);
    int64_t t_index_delvec = MonotonicMillis();

    {
        std::lock_guard wl(_lock);
        // 3. write meta
        st = TabletMetaManager::apply_rowset_commit(_tablet.data_dir(), tablet_id, _next_log_id, version_info.version,
                                                    delvecs);
        if (!st.ok()) {
            LOG(ERROR) << "_apply_compaction_commit error: write meta failed: " << st << " " << _debug_string(false);
            manager->index_cache().release(index_entry);
            _set_error();
            return;
        }
        // 4. put delvec in cache
        TabletSegmentId tsid;
        tsid.tablet_id = _tablet.tablet_id();
        for (auto& delvec_pair : delvecs) {
            tsid.segment_id = delvec_pair.first;
            manager->set_cached_del_vec(tsid, delvec_pair.second);
        }
        // 5. apply memory
        _next_log_id++;
        _apply_version_idx++;
        _apply_version_changed.notify_all();
    }
    {
        // Update the stats of affected rowsets.
        std::lock_guard lg(_rowset_stats_lock);
        auto iter = _rowset_stats.find(rowset_id);
        if (iter == _rowset_stats.end()) {
            string msg = Substitute("inconsistent rowset_stats, rowset not found tablet=$0 rowsetid=$1 $2",
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
    _update_total_stats(version_info.rowsets);
    int64_t t_write = MonotonicMillis();
    size_t del_percent = _cur_total_rows == 0 ? 0 : (_cur_total_dels * 100) / _cur_total_rows;
    LOG(INFO) << "apply_compaction_commit finish tablet:" << tablet_id
              << " version:" << version_info.version.to_string() << " total del/row:" << _cur_total_dels << "/"
              << _cur_total_rows << " " << del_percent << "%"
              << " rowset:" << rowset_id << " #row:" << total_rows << " #del:" << total_deletes
              << " #delvec:" << delvecs.size() << " duration:" << t_write - t_start << "ms"
              << Substitute("($0/$1/$2)", t_load - t_start, t_index_delvec - t_load, t_write - t_index_delvec);
    VLOG(1) << "update compaction apply " << _debug_string(true, true);
}

void TabletUpdates::to_updates_pb(TabletUpdatesPB* updates_pb) const {
    std::lock_guard rl(_lock);
    _to_updates_pb_unlocked(updates_pb);
}

void TabletUpdates::_erase_expired_versions(int64_t expire_time,
                                            std::vector<std::unique_ptr<EditVersionInfo>>* expire_list) {
    DCHECK(expire_list->empty());
    std::lock_guard l(_lock);
    for (int i = 0; i < _apply_version_idx; i++) {
        if (_edit_version_infos[i]->creation_time <= expire_time) {
            expire_list->emplace_back(std::move(_edit_version_infos[i]));
        } else {
            break;
        }
    }
    auto n = expire_list->size();
    _edit_version_infos.erase(_edit_version_infos.begin(), _edit_version_infos.begin() + n);
    _apply_version_idx -= n;
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

std::set<uint32_t> TabletUpdates::_active_rowsets() {
    std::set<uint32_t> ret;
    std::lock_guard rl(_lock);
    for (const auto& edit_version_info : _edit_version_infos) {
        ret.insert(edit_version_info->rowsets.begin(), edit_version_info->rowsets.end());
    }
    return ret;
}

void TabletUpdates::remove_expired_versions(int64_t expire_time) {
    if (_error) {
        LOG(WARNING) << Substitute("remove_expired_versions failed, tablet updates is in error state: tablet:$0 $1",
                                   _tablet.tablet_id(), _error_msg);
        return;
    }
    /// Remove expired versions from memory.
    std::vector<std::unique_ptr<EditVersionInfo>> expired_edit_version_infos;
    _erase_expired_versions(expire_time, &expired_edit_version_infos);

    if (!expired_edit_version_infos.empty()) {
        std::unique_lock wrlock(_tablet.get_header_lock());
        _tablet.save_meta();

        std::set<uint32_t> unused_rid;
        std::set<uint32_t> active_rid = _active_rowsets();
        for (const auto& expired_edit_version_info : expired_edit_version_infos) {
            VLOG(1) << "Removing expired version " << expired_edit_version_info->version.to_string() << " of tablet "
                    << _tablet.tablet_id();
            for (uint32_t expired_rid : expired_edit_version_info->rowsets) {
                if (active_rid.count(expired_rid) == 0) {
                    unused_rid.insert(expired_rid);
                }
            }
        }
        for (uint32_t id : unused_rid) {
            std::lock_guard l(_rowsets_lock);
            auto iter = _rowsets.find(id);
            DCHECK(iter != _rowsets.end());
            (void)_unused_rowsets.blocking_put(std::move(iter->second));
            _rowsets.erase(iter);
        }
        for (uint32_t id : unused_rid) {
            std::lock_guard l(_rowset_stats_lock);
            _rowset_stats.erase(id);
        }

        /// Remove useless delete vectors.
        auto max_expired_version = expired_edit_version_infos.back()->version.major();
        auto meta_store = _tablet.data_dir()->get_meta();
        auto tablet_id = _tablet.tablet_id();

        size_t n_delvec_range = 0;
        auto res = TabletMetaManager::list_del_vector(meta_store, tablet_id, max_expired_version + 1);
        if (res.ok()) {
            for (const auto& elem : *res) {
                auto segment_id = elem.first;
                auto end_version = elem.second;
                (void)TabletMetaManager::delete_del_vector_range(meta_store, tablet_id, segment_id, 0, end_version);
                VLOG(1) << "Removed delete vector tablet_id=" << tablet_id << " segment_id=" << segment_id
                        << " start_version=0 end_version=" << end_version;
            }
            n_delvec_range = (*res).size();
        } else {
            LOG(WARNING) << "Fail to list delete vector: " << res.status();
        }
        LOG(INFO) << Substitute(
                "remove_expired_versions $0 time:$1 max_expire_version:$2 deletes: #version:$3 #rowset:$4 "
                "#delvecrange:$5",
                _debug_version_info(true), expire_time, max_expired_version, expired_edit_version_infos.size(),
                unused_rid.size(), n_delvec_range);
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
        if (_apply_version_idx + 2 < _edit_version_infos.size() || _pending_commits.size() >= 2) {
            // has too many pending tasks, skip compaction
            return -1;
        }
        for (size_t i = _apply_version_idx + 1; i < _edit_version_infos.size(); i++) {
            if (_edit_version_infos[i]->compaction) {
                // has pending compaction not finished, do not do compaction
                return -1;
            }
        }
        rowsets = _edit_version_infos[_apply_version_idx]->rowsets;
    }
    int64_t total_score = -_compaction_cost_seek;
    bool has_error = false;
    {
        std::lock_guard lg(_rowset_stats_lock);
        for (auto rowsetid : rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr == _rowset_stats.end()) {
                // should not happen
                string msg = Substitute("rowset not found in rowset stats tablet=$0 rowset=$1", _tablet.tablet_id(),
                                        rowsetid);
                DCHECK(false) << msg;
                LOG(WARNING) << msg;
                has_error = true;
            } else if (itr->second->compaction_score > 0) {
                total_score += itr->second->compaction_score;
            }
        }
    }
    if (has_error) {
        LOG(WARNING) << "error get_compaction_score: " << debug_string();
        // do not do compaction
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

static const size_t compaction_result_bytes_threashold = 1000000000;
static const size_t compaction_result_rows_threashold = 10000000;

Status TabletUpdates::compaction(MemTracker* mem_tracker) {
    if (_error) {
        return Status::InternalError(Substitute("compaction failed, tablet updates is in error state: tablet:$0 $1",
                                                _tablet.tablet_id(), _error_msg));
    }
    bool was_runing = false;
    if (!_compaction_running.compare_exchange_strong(was_runing, true)) {
        return Status::InternalError("illegal state: another compaction is running");
    }
    std::unique_ptr<CompactionInfo> info = std::make_unique<CompactionInfo>();
    vector<uint32_t> rowsets;
    {
        std::lock_guard rl(_lock);
        // 1. start compaction at current apply version
        info->start_version = _edit_version_infos[_apply_version_idx]->version;
        rowsets = _edit_version_infos[_apply_version_idx]->rowsets;
    }
    size_t total_valid_rowsets = 0;
    size_t total_rows = 0;
    size_t total_bytes = 0;
    size_t total_rows_after_compaction = 0;
    size_t total_bytes_after_compaction = 0;
    int64_t total_score = -_compaction_cost_seek;
    vector<CompactionEntry> candidates;
    {
        std::lock_guard lg(_rowset_stats_lock);
        for (auto rowsetid : rowsets) {
            auto itr = _rowset_stats.find(rowsetid);
            if (itr == _rowset_stats.end()) {
                // should not happen
                string msg = Substitute("rowset not found in rowset stats tablet=$0 rowset=$1", _tablet.tablet_id(),
                                        rowsetid);
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
                    continue;
                }
                candidates.emplace_back();
                auto& e = candidates.back();
                e.rowsetid = itr->first;
                e.score_per_row = (float)((double)stat.compaction_score / (stat.num_rows - stat.num_dels));
                e.num_rows = stat.num_rows;
                e.num_dels = stat.num_dels;
                e.bytes = stat.byte_size;
            }
        }
    }
    std::sort(candidates.begin(), candidates.end());
    for (auto& e : candidates) {
        size_t new_rows = total_rows_after_compaction + e.num_rows - e.num_dels;
        size_t new_bytes = total_bytes_after_compaction + e.bytes * (e.num_rows - e.num_dels) / e.num_rows;
        if (info->inputs.size() > 0 && (new_rows > compaction_result_rows_threashold * 3 / 2 ||
                                        new_bytes > compaction_result_bytes_threashold * 3 / 2)) {
            break;
        }
        info->inputs.push_back(e.rowsetid);
        total_score += e.score_per_row * (e.num_rows - e.num_dels);
        total_rows += e.num_rows;
        total_bytes += e.bytes;
        total_rows_after_compaction = new_rows;
        total_bytes_after_compaction = new_bytes;
        if (total_bytes_after_compaction > compaction_result_bytes_threashold ||
            total_rows_after_compaction > compaction_result_rows_threashold) {
            break;
        }
    }
    if (total_valid_rowsets - info->inputs.size() <= 3) {
        // give 10s time gitter, so same table's compaction don't start at same time
        _last_compaction_time_ms = UnixMillis() + rand() % 10000;
    }
    std::sort(info->inputs.begin(), info->inputs.end());
    // else there are still many(>3) rowset's need's to be compacted,
    // do not reset _last_compaction_time_ms so we can continue doing compaction
    LOG(INFO) << "update compaction start tablet:" << _tablet.tablet_id()
              << " version:" << info->start_version.to_string() << " score:" << total_score
              << " pick:" << info->inputs.size() << "/valid:" << total_valid_rowsets << "/all:" << rowsets.size() << " "
              << int_list_to_string(info->inputs) << " #rows:" << total_rows << "->" << total_rows_after_compaction
              << " bytes:" << PrettyPrinter::print(total_bytes, TUnit::BYTES) << "->"
              << PrettyPrinter::print(total_bytes_after_compaction, TUnit::BYTES) << "(estimate)";

    MemTracker* prev_tracker = tls_thread_status.set_mem_tracker(mem_tracker);
    DeferOp op([&] { tls_thread_status.set_mem_tracker(prev_tracker); });

    Status st = _do_compaction(&info, true);
    if (!st.ok()) {
        _compaction_running = false;
    }
    return st;
}

void TabletUpdates::_calc_compaction_score(RowsetStats* stats) {
    if (stats->num_rows < 10) {
        stats->compaction_score = _compaction_cost_seek;
        return;
    }
    // TODO(cbl): estimate read/write cost, currently just use fixed value
    const int64_t cost_record_write = 1;
    const int64_t cost_record_read = 4;
    // use double to prevent overflow
    int64_t delete_bytes = (int64_t)(stats->byte_size * (double)stats->num_dels / stats->num_rows);
    stats->compaction_score = _compaction_cost_seek + (cost_record_read + cost_record_write) * delete_bytes -
                              cost_record_write * stats->byte_size;
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
    int64_t version;
    vector<uint32_t> rowsets;
    {
        std::lock_guard rl(_lock);
        auto& last = _edit_version_infos.back();
        version = last->version.major();
        rowsets = last->rowsets;
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
    info->__set_version_count(rowsets.size());
    info->__set_row_count(total_row);
    info->__set_data_size(total_size);
}

std::string TabletUpdates::RowsetStats::to_string() const {
    return Substitute("[seg:$0 row:$1 del:$2 bytes:$3 compaction:$4]", num_segments, num_rows, num_dels, byte_size,
                      compaction_score);
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
    apply_idx = _apply_version_idx;
    first_version = _edit_version_infos[0]->version;
    apply_version = _edit_version_infos[_apply_version_idx]->version;
    last_version = _edit_version_infos.back()->version;
    rowsets = _edit_version_infos.back()->rowsets;
    for (auto const& pending_commit : _pending_commits) {
        StringAppendF(&pending_info, "%ld,", pending_commit.first);
    }
    if (lock) _lock.unlock();

    std::string ret = Substitute("tablet:$0 #version:$1 [$2 $3@$4 $5] pending:$6 rowsets:$7", _tablet.tablet_id(),
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
    apply_idx = _apply_version_idx;
    first_version = _edit_version_infos[0]->version;
    apply_version = _edit_version_infos[_apply_version_idx]->version;
    last_version = _edit_version_infos.back()->version;
    npending = _pending_commits.size();
    if (lock) _lock.unlock();

    std::string ret = Substitute("tablet:$0 #version:$1 [$2 $3@$4 $5] #pending:$6", _tablet.tablet_id(), num_version,
                                 first_version.to_string(), apply_version.to_string(), apply_idx,
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
                string compaction = PrettyPrinter::print(abs(stats.compaction_score), TUnit::BYTES);
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

void TabletUpdates::_set_error() {
    _error = true;
    _apply_version_changed.notify_all();
}

RowsetSharedPtr TabletUpdates::get_delta_rowset(int64_t version) const {
    if (_error) {
        LOG(WARNING) << Substitute("get_delta_rowset failed, tablet updates is in error state: tablet:$0 $1",
                                   _tablet.tablet_id(), _error_msg);
        return nullptr;
    }
    std::lock_guard lg(_lock);
    if (version < _edit_version_infos[0]->version.major() || _edit_version_infos.back()->version.major() < version) {
        return nullptr;
    }
    int idx_hint = version - _edit_version_infos[0]->version.major();
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
                Substitute("get_applied_rowsets failed, tablet updates is in error state: tablet:$0 $1",
                           _tablet.tablet_id(), _error_msg));
    }
    // TODO(cbl): optimize: following code lock _lock twice, should make it just lock once
    RETURN_IF_ERROR(_wait_for_version(EditVersion(version, 0), 60000));
    std::lock_guard rl(_lock);
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
                    string msg = Substitute("get_rowsets rowset not found: version:$0 rowset:$1 $2", version, rsid,
                                            _debug_string(false, true));
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
};

Status TabletUpdates::link_from(Tablet* base_tablet, int64_t request_version) {
    OlapStopWatch watch;
    DCHECK(_tablet.tablet_state() == TABLET_NOTREADY)
            << "tablet state is not TABLET_NOTREADY, link_from is not allowed"
            << " tablet_id:" << _tablet.tablet_id() << " tablet_state:" << _tablet.tablet_state();
    LOG(INFO) << "link_from start tablet:" << _tablet.tablet_id() << " #pending:" << _pending_commits.size()
              << " base_tablet:" << base_tablet->tablet_id() << " request_version:" << request_version;
    int64_t max_version = base_tablet->updates()->max_version();
    if (max_version < request_version) {
        LOG(WARNING) << "link_from: base_tablet's max_version:" << max_version << " < alter_version:" << request_version
                     << " tablet:" << _tablet.tablet_id() << " base_tablet:" << base_tablet->tablet_id();
        return Status::InternalError("link_from: max_version < request version");
    }
    vector<RowsetSharedPtr> rowsets;
    EditVersion version;
    Status st = base_tablet->updates()->get_applied_rowsets(request_version, &rowsets, &version);
    if (!st.ok()) {
        LOG(WARNING) << "link_from: get base tablet rowsets error tablet:" << base_tablet->tablet_id()
                     << " version:" << request_version << " reason:" << st;
        return st;
    }

    // disable compaction temporarily when tablet just loaded
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
        auto st = src_rowset.link_files_to(_tablet.schema_hash_path(), rid);
        if (!st.ok()) {
            return st;
        }
        auto& new_rowset_info = new_rowsets[i];
        new_rowset_info.rowset_id = next_rowset_id;
        new_rowset_info.num_segments = src_rowset.num_segments();
        // use src_rowset's meta as base, change some fields to new tablet
        auto& rowset_meta_pb = new_rowset_info.rowset_meta_pb;
        src_rowset.rowset_meta()->to_rowset_pb(&rowset_meta_pb);
        rowset_meta_pb.set_rowset_id(0);
        rowset_meta_pb.set_rowset_id_v2(rid.to_string());
        rowset_meta_pb.set_rowset_seg_id(new_rowset_info.rowset_id);
        rowset_meta_pb.set_partition_id(_tablet.tablet_meta()->partition_id());
        rowset_meta_pb.set_tablet_id(tablet_id);
        rowset_meta_pb.set_tablet_schema_hash(_tablet.schema_hash());
        new_rowset_info.delvecs.resize(new_rowset_info.num_segments);
        for (uint32_t j = 0; j < new_rowset_info.num_segments; j++) {
            TabletSegmentId tsid;
            tsid.tablet_id = src_rowset.rowset_meta()->tablet_id();
            tsid.segment_id = src_rowset.rowset_meta()->get_rowset_seg_id() + j;
            Status st = update_manager->get_del_vec(kv_store, tsid, version.major(), &new_rowset_info.delvecs[j]);
            if (!st.ok()) {
                return st;
            }
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
    // do not clear pending rowsets, because these pending rowsets should be committed after schemachange is done
    RETURN_IF_ERROR(TabletMetaManager::put_tablet_meta(data_dir, &wb, meta_pb));
    for (auto& info : new_rowsets) {
        RETURN_IF_ERROR(TabletMetaManager::put_rowset_meta(data_dir, &wb, tablet_id, info.rowset_meta_pb));
        for (int j = 0; j < info.num_segments; j++) {
            RETURN_IF_ERROR(
                    TabletMetaManager::put_del_vector(data_dir, &wb, tablet_id, info.rowset_id + j, *info.delvecs[j]));
        }
    }

    std::unique_lock wrlock(_tablet.get_header_lock());
    st = kv_store->write_batch(&wb);
    if (!st.ok()) {
        LOG(WARNING) << "Fail to delete old meta and write new meta" << tablet_id << ": " << st;
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
        LOG(WARNING) << "_load_from_pb failed tablet_id:" << tablet_id << " " << st;
        return st;
    }
    _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
    LOG(INFO) << "link_from finish tablet:" << _tablet.tablet_id() << " version:" << this->max_version()
              << " base tablet:" << base_tablet->tablet_id() << " #pending:" << _pending_commits.size()
              << " time:" << watch.get_elapse_second() << "s"
              << " #rowset:" << rowsets.size() << " #file:" << total_files << " #row:" << total_rows
              << " bytes:" << total_bytes;
    return Status::OK();
}

Status TabletUpdates::convert_from(const std::shared_ptr<Tablet>& base_tablet, int64_t request_version,
                                   vectorized::ChunkChanger* chunk_changer) {
    OlapStopWatch watch;
    DCHECK(_tablet.tablet_state() == TABLET_NOTREADY)
            << "tablet state is not TABLET_NOTREADY, convert_from is not allowed"
            << " tablet_id:" << _tablet.tablet_id() << " tablet_state:" << _tablet.tablet_state();
    LOG(INFO) << "convert_from start tablet:" << _tablet.tablet_id() << " #pending:" << _pending_commits.size()
              << " base_tablet:" << base_tablet->tablet_id() << " request_version:" << request_version;
    int64_t max_version = base_tablet->updates()->max_version();
    if (max_version < request_version) {
        LOG(WARNING) << "convert_from: base_tablet's max_version:" << max_version
                     << " < alter_version:" << request_version << " tablet:" << _tablet.tablet_id()
                     << " base_tablet:" << base_tablet->tablet_id();
        return Status::InternalError("convert_from: max_version < request_version");
    }
    std::vector<RowsetSharedPtr> src_rowsets;
    EditVersion version;
    Status status = base_tablet->updates()->get_applied_rowsets(request_version, &src_rowsets, &version);
    if (!status.ok()) {
        LOG(WARNING) << "convert_from: get base tablet rowsets error tablet:" << base_tablet->tablet_id()
                     << " request_version:" << request_version << " reason:" << status;
        return status;
    }

    // disable compaction temporarily when tablet just loaded
    _last_compaction_time_ms = UnixMillis();

    auto kv_store = _tablet.data_dir()->get_meta();
    auto tablet_id = _tablet.tablet_id();
    uint32_t next_rowset_id = 0;
    std::vector<RowsetLoadInfo> new_rowset_load_infos(src_rowsets.size());

    vectorized::Schema base_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(base_tablet->tablet_schema());

    OlapReaderStatistics stats;

    size_t total_bytes = 0;
    size_t total_rows = 0;
    size_t total_files = 0;
    for (int i = 0; i < src_rowsets.size(); i++) {
        const auto& src_rowset = src_rowsets[i];

        RowsetReleaseGuard guard(src_rowset->shared_from_this());
        auto beta_rowset = down_cast<BetaRowset*>(src_rowset.get());
        auto res = beta_rowset->get_segment_iterators2(base_schema, base_tablet->data_dir()->get_meta(),
                                                       version.major(), &stats);
        if (!res.ok()) {
            return res.status();
        }

        RowsetId rid = StorageEngine::instance()->next_rowset_id();

        RowsetWriterContext writer_context(kDataFormatUnknown, config::storage_format_version);
        writer_context.rowset_id = rid;
        writer_context.tablet_uid = _tablet.tablet_uid();
        writer_context.tablet_id = _tablet.tablet_id();
        writer_context.partition_id = _tablet.partition_id();
        writer_context.tablet_schema_hash = _tablet.schema_hash();
        writer_context.rowset_type = _tablet.tablet_meta()->preferred_rowset_type();
        writer_context.rowset_path_prefix = _tablet.schema_hash_path();
        writer_context.tablet_schema = &_tablet.tablet_schema();
        writer_context.rowset_state = VISIBLE;
        writer_context.version = src_rowset->version();
        writer_context.segments_overlap = src_rowset->rowset_meta()->segments_overlap();

        std::unique_ptr<RowsetWriter> rowset_writer;
        status = RowsetFactory::create_rowset_writer(writer_context, &rowset_writer);
        if (!status.ok()) {
            LOG(INFO) << "build rowset writer failed";
            return Status::InternalError("build rowset writer failed");
        }

        // notice: rowset's del files not linked, it's not useful
        status = _convert_from_base_rowset(base_tablet, res.value(), chunk_changer, rowset_writer);
        if (!status.ok()) {
            LOG(WARNING) << "failed to convert from base rowset, exit alter process";
            return status;
        }

        auto new_rowset = rowset_writer->build();
        if (!new_rowset.ok()) return new_rowset.status();

        auto& new_rowset_load_info = new_rowset_load_infos[i];
        new_rowset_load_info.num_segments = (*new_rowset)->num_segments();
        new_rowset_load_info.rowset_id = next_rowset_id;

        auto& rowset_meta_pb = new_rowset_load_info.rowset_meta_pb;
        (*new_rowset)->rowset_meta()->to_rowset_pb(&rowset_meta_pb);
        rowset_meta_pb.set_rowset_seg_id(new_rowset_load_info.rowset_id);
        rowset_meta_pb.set_rowset_id_v2(rid.to_string());

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
    status = kv_store->write_batch(&wb);
    if (!status.ok()) {
        LOG(WARNING) << "Fail to delete old meta and write new meta" << tablet_id << ": " << status;
        return Status::InternalError("Fail to delete old meta and write new meta");
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
        LOG(WARNING) << "_load_from_pb failed tablet_id:" << tablet_id << " " << status;
        return status;
    }

    _tablet.set_tablet_state(TabletState::TABLET_RUNNING);
    LOG(INFO) << "convert_from finish tablet:" << _tablet.tablet_id() << " version:" << this->max_version()
              << " base tablet:" << base_tablet->tablet_id() << " #pending:" << _pending_commits.size()
              << " time:" << watch.get_elapse_second() << "s"
              << " #column:" << _tablet.tablet_schema().num_columns() << " #rowset:" << src_rowsets.size()
              << " #file:" << total_files << " #row:" << total_rows << " bytes:" << total_bytes;
    return Status::OK();
}

Status TabletUpdates::_convert_from_base_rowset(const std::shared_ptr<Tablet>& base_tablet,
                                                const std::vector<vectorized::ChunkIteratorPtr>& seg_iterators,
                                                vectorized::ChunkChanger* chunk_changer,
                                                const std::unique_ptr<RowsetWriter>& rowset_writer) {
    vectorized::Schema base_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(base_tablet->tablet_schema());
    vectorized::ChunkPtr base_chunk = vectorized::ChunkHelper::new_chunk(base_schema, config::vector_chunk_size);

    vectorized::Schema new_schema = vectorized::ChunkHelper::convert_schema_to_format_v2(_tablet.tablet_schema());
    vectorized::ChunkPtr new_chunk = vectorized::ChunkHelper::new_chunk(new_schema, config::vector_chunk_size);

    std::unique_ptr<MemPool> mem_pool(new MemPool());

    for (auto& seg_iterator : seg_iterators) {
        if (seg_iterator.get() == nullptr) {
            continue;
        }
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
            RETURN_IF_ERROR(rowset_writer->add_chunk(*new_chunk));
        }
    }

    return rowset_writer->flush();
}

void TabletUpdates::_remove_unused_rowsets() {
    size_t removed = 0;
    std::vector<RowsetSharedPtr> skipped_rowsets;
    RowsetSharedPtr rowset;
    while (_unused_rowsets.try_get(&rowset) == 1) {
        if (rowset.use_count() > 1) {
            LOG(WARNING) << "rowset " << rowset->rowset_id() << " still been referenced"
                         << " tablet:" << _tablet.tablet_id();
            skipped_rowsets.emplace_back(std::move(rowset));
            continue;
        }

        _clear_rowset_del_vec_cache(*rowset);

        Status st =
                TabletMetaManager::rowset_delete(_tablet.data_dir(), _tablet.tablet_id(),
                                                 rowset->rowset_meta()->get_rowset_seg_id(), rowset->num_segments());
        if (!st.ok()) {
            LOG(WARNING) << "Fail to delete rowset " << rowset->rowset_id() << ": " << st
                         << " tablet:" << _tablet.tablet_id();
            skipped_rowsets.emplace_back(std::move(rowset));
            continue;
        }
        rowset->close();
        rowset->set_need_delete_file();
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
            svpb->set_major(cp->start_version.minor());
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

Status TabletUpdates::load_snapshot(const SnapshotMeta& snapshot_meta) {
#define CHECK_FAIL(status)    \
    do {                      \
        Status st = (status); \
        if (!st.ok()) {       \
            LOG(ERROR) << st; \
            _set_error();     \
            return st;        \
        }                     \
    } while (0)

    if (_error.load()) {
        return Status::InternalError(Substitute("load snapshot failed, tablet updates is in error state: tablet:$0 $1",
                                                _tablet.tablet_id(), _error_msg));
    }
    // disable compaction temporarily when doing load_snapshot
    _last_compaction_time_ms = UnixMillis();

    // A utility function used to ensure that segment files have been placed under the
    // tablet directory.
    auto check_rowset_files = [&](const RowsetMetaPB& rowset) {
        for (int seg_id = 0; seg_id < rowset.num_segments(); seg_id++) {
            RowsetId rowset_id;
            rowset_id.init(rowset.rowset_id_v2());
            auto path = BetaRowset::segment_file_path(_tablet.schema_hash_path(), rowset_id, seg_id);
            auto st = Env::Default()->path_exists(path);
            if (!st.ok()) {
                return Status::InternalError("segment file does not exist: " + st.to_string());
            }
        }
        for (int del_id = 0; del_id < rowset.num_delete_files(); del_id++) {
            RowsetId rowset_id;
            rowset_id.init(rowset.rowset_id_v2());
            auto path = BetaRowset::segment_del_file_path(_tablet.schema_hash_path(), rowset_id, del_id);
            auto st = Env::Default()->path_exists(path);
            if (!st.ok()) {
                return Status::InternalError("delete file does not exist: " + st.to_string());
            }
        }
        return Status::OK();
    };

    if (snapshot_meta.snapshot_type() == SNAPSHOT_TYPE_INCREMENTAL) {
        // Assume the elements of |snapshot_meta.rowset_metas()| are sorted by version.
        for (const auto& rowset_meta_pb : snapshot_meta.rowset_metas()) {
            RETURN_IF_ERROR(check_rowset_files(rowset_meta_pb));
            RowsetSharedPtr rowset;
            auto rowset_meta = std::make_shared<RowsetMeta>();
            if (!rowset_meta->init_from_pb(rowset_meta_pb)) {
                return Status::InternalError("rowset meta init from pb failed");
            }
            if (rowset_meta->tablet_id() != _tablet.tablet_id()) {
                return Status::InternalError("mismatched tablet id");
            }
            RETURN_IF_ERROR(RowsetFactory::create_rowset(&_tablet.tablet_schema(), _tablet.schema_hash_path(),
                                                         rowset_meta, &rowset));
            if (rowset->start_version() != rowset->end_version()) {
                return Status::InternalError("mismatched start and end version");
            }
            RETURN_IF_ERROR(rowset_commit(rowset->end_version(), rowset));
        }
        return Status::OK();
    } else if (snapshot_meta.snapshot_type() == SNAPSHOT_TYPE_FULL) {
        if (snapshot_meta.tablet_meta().tablet_id() != _tablet.tablet_id()) {
            return Status::InvalidArgument("mismatched tablet id");
        }
        if (snapshot_meta.tablet_meta().schema_hash() != _tablet.schema_hash()) {
            return Status::InvalidArgument("mismatched schema hash");
        }
        if (snapshot_meta.snapshot_version() <= _edit_version_infos.back()->version.major()) {
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
        std::unique_lock l2(_rowsets_lock);
        std::unique_lock l3(_rowset_stats_lock);

        // Check version again after lock acquired.
        if (snapshot_meta.snapshot_version() <= _edit_version_infos.back()->version.major()) {
            return Status::Cancelled("snapshot version too small");
        }

        uint32_t new_next_rowset_id = _next_rowset_id;
        for (const auto& rowset_meta_pb : snapshot_meta.rowset_metas()) {
            RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
            if (!rowset_meta->init_from_pb(rowset_meta_pb)) {
                return Status::InternalError("fail to init rowset meta");
            }
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
        for (const auto& [rid, rowset] : _rowsets) {
            RowsetMetaPB meta_pb = rowset->rowset_meta()->to_rowset_pb();
            CHECK_FAIL(TabletMetaManager::put_rowset_meta(data_store, &wb, tablet_id, meta_pb));
        }

        _next_rowset_id = new_next_rowset_id;

        _to_updates_pb_unlocked(new_tablet_meta_pb.mutable_updates());
        VLOG(2) << new_tablet_meta_pb.updates().DebugString();
        CHECK_FAIL(TabletMetaManager::put_tablet_meta(data_store, &wb, new_tablet_meta_pb));

        if (!meta_store->write_batch(&wb).ok()) {
            LOG(ERROR) << "Fail to put write batch";
            _set_error();
            return Status::InternalError("fail to put write batch");
        }

        for (const auto& [rid, rowset] : _rowsets) {
            auto stats = std::make_unique<RowsetStats>();
            stats->num_segments = rowset->num_segments();
            stats->num_rows = rowset->num_rows();
            stats->byte_size = rowset->data_disk_size();
            stats->num_dels = _get_rowset_num_deletes(*rowset);
            _calc_compaction_score(stats.get());
            _rowset_stats.emplace(rid, std::move(stats));
        }

        l3.unlock();                     // _rowset_stats_lock
        l2.unlock();                     // _rowsets_lock
        _try_commit_pendings_unlocked(); // may acquire |_rowset_stats_lock| and |_rowsets_lock|

        // unload primary index
        auto manager = StorageEngine::instance()->update_manager();
        auto& index_cache = manager->index_cache();
        auto index_entry = index_cache.get_or_create(tablet_id);
        index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
        index_entry->value().unload();
        index_cache.release(index_entry);

        _apply_version_changed.notify_all();
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

Status TabletUpdates::clear_meta() {
    std::lock_guard l1(_lock);
    std::lock_guard l2(_rowsets_lock);
    std::lock_guard l3(_rowset_stats_lock);
    _remove_unused_rowsets();
    if (_unused_rowsets.get_size() != 0) {
        return Status::InternalError("some unused rowset cannot be removed");
    }

    WriteBatch wb;
    auto data_store = _tablet.data_dir();
    auto meta_store = data_store->get_meta();

    _set_error(); // Mark this tablet unusable first.

    // Clear permanently stored meta.
    TabletMetaManager::clear_pending_rowset(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::clear_rowset(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::clear_del_vector(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::clear_log(data_store, &wb, _tablet.tablet_id());
    TabletMetaManager::remove_tablet_meta(data_store, &wb, _tablet.tablet_id(), _tablet.schema_hash());
    RETURN_IF_ERROR(meta_store->write_batch(&wb));

    // Clear cached delete vectors.
    for (auto& [id, rowset] : _rowsets) {
        _clear_rowset_del_vec_cache(*rowset);
    }
    // Clear cached primary index.
    StorageEngine::instance()->update_manager()->index_cache().remove_by_key(_tablet.tablet_id());
    STLClearObject(&_rowsets);
    STLClearObject(&_rowset_stats);
    STLClearObject(&_edit_version_infos);
    return Status::OK();
}

void TabletUpdates::_update_total_stats(const std::vector<uint32_t>& rowsets) {
    size_t nrow = 0;
    size_t ndel = 0;
    {
        std::lock_guard l(_rowset_stats_lock);
        for (auto rid : rowsets) {
            auto itr = _rowset_stats.find(rid);
            if (itr != _rowset_stats.end()) {
                nrow += itr->second->num_rows;
                ndel += itr->second->num_dels;
            }
        }
    }
    _cur_total_rows = nrow;
    _cur_total_dels = ndel;
}

Status TabletUpdates::get_column_values(std::vector<uint32_t>& column_ids, bool with_default,
                                        std::map<uint32_t, std::vector<uint32_t>>& rowids_by_rssid,
                                        vector<std::unique_ptr<vectorized::Column>>* columns) {
    std::map<uint32_t, RowsetSharedPtr> rssid_to_rowsets;
    {
        std::lock_guard<std::mutex> l(_rowsets_lock);
        for (const auto& rowset : _rowsets) {
            rssid_to_rowsets.insert(rowset);
        }
    }
    if (with_default) {
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
    for (const auto& [rssid, rowids] : rowids_by_rssid) {
        auto iter = rssid_to_rowsets.upper_bound(rssid);
        --iter;
        const auto& rowset = iter->second.get();
        if (!(rowset->rowset_meta()->get_rowset_seg_id() <= rssid &&
              rssid < rowset->rowset_meta()->get_rowset_seg_id() + rowset->num_segments())) {
            std::string msg = Substitute("illegal rssid: $0, should in [$1, $2)", rssid,
                                         rowset->rowset_meta()->get_rowset_seg_id(),
                                         rowset->rowset_meta()->get_rowset_seg_id() + rowset->num_segments());
            LOG(ERROR) << msg;
            _set_error();
            return Status::InternalError(msg);
        }
        std::string seg_path =
                BetaRowset::segment_file_path(rowset->rowset_path(), rowset->rowset_id(), rssid - iter->first);
        auto segment = Segment::open(ExecEnv::GetInstance()->tablet_meta_mem_tracker(), fs::fs_util::block_manager(),
                                     seg_path, rssid - iter->first, &rowset->schema());
        if (!segment.ok()) {
            LOG(WARNING) << "Fail to open " << seg_path << ": " << segment.status();
            return segment.status();
        }
        if ((*segment)->num_rows() == 0) {
            continue;
        }
        ColumnIteratorOptions iter_opts;
        OlapReaderStatistics stats;
        iter_opts.stats = &stats;
        std::unique_ptr<fs::ReadableBlock> rblock;
        RETURN_IF_ERROR(fs::fs_util::block_manager()->open_block((*segment)->file_name(), &rblock));
        iter_opts.rblock = rblock.get();
        for (auto i = 0; i < column_ids.size(); ++i) {
            ColumnIterator* col_iter_raw_ptr = nullptr;
            RETURN_IF_ERROR((*segment)->new_column_iterator(column_ids[i], &col_iter_raw_ptr));
            std::unique_ptr<ColumnIterator> col_iter(col_iter_raw_ptr);
            RETURN_IF_ERROR(col_iter->init(iter_opts));
            RETURN_IF_ERROR(col_iter->fetch_values_by_rowid(rowids.data(), rowids.size(), (*columns)[i].get()));
        }
    }
    return Status::OK();
}

Status TabletUpdates::prepare_partial_update_states(Tablet* tablet, const std::vector<ColumnUniquePtr>& upserts,
                                                    EditVersion* read_version, uint32_t* next_rowset_id,
                                                    std::vector<std::vector<uint64_t>*>* rss_rowids) {
    std::lock_guard lg(_index_lock);
    {
        // get next_rowset_id and read_version to identify conflict
        std::lock_guard wl(_lock);
        *next_rowset_id = _next_rowset_id;
        *read_version = _edit_version_infos[_apply_version_idx]->version;
    }

    auto manager = StorageEngine::instance()->update_manager();
    auto index_entry = manager->index_cache().get_or_create(tablet->tablet_id());
    index_entry->update_expire_time(MonotonicMillis() + manager->get_cache_expire_ms());
    auto& index = index_entry->value();
    auto st = index.load(tablet);
    manager->index_cache().update_object_size(index_entry, index.memory_usage());
    if (!st.ok()) {
        LOG(ERROR) << "prepare_partial_update_states error: load primary index failed: " << st << " " << debug_string();
        manager->index_cache().remove(index_entry);
        _set_error();
        return Status::InternalError("prepare_partial_update_states error: load primary index failed");
    }

    // get rss_rowids for each segment of rowset
    uint32_t num_segments = upserts.size();
    for (size_t i = 0; i < num_segments; i++) {
        auto& pks = *upserts[i];
        index.get(pks, (*rss_rowids)[i]);
    }
    // index may be used for later commits, keep in cache
    manager->index_cache().release(index_entry);

    return Status::OK();
}

} // namespace starrocks
