// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/morsel.h"

#include "exec/olap_utils.h"
#include "exec/pipeline/scan/scan_operator.h"
#include "exec/scan_node.h"
#include "gen_cpp/PlanNodes_types.h"
#include "storage/range.h"
#include "storage/rowset/beta_rowset.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/storage_engine.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"

namespace starrocks {
namespace pipeline {

/// Morsel.
void PhysicalSplitScanMorsel::init_tablet_reader_params(vectorized::TabletReaderParams* params) {
    params->rowid_range_option = _rowid_range_option;
}

/// MorselQueue.
void _convert_morsels_to_olap_scan_ranges(const Morsels& morsels, std::vector<TInternalScanRange*>* scan_ranges) {
    scan_ranges->reserve(morsels.size());
    for (const auto& morsel : morsels) {
        auto* scan_morsel = down_cast<ScanMorsel*>(morsel.get());
        auto* scan_range = scan_morsel->get_olap_scan_range();
        scan_ranges->emplace_back(scan_range);
    }
}

std::vector<TInternalScanRange*> FixedMorselQueue::olap_scan_ranges() const {
    std::vector<TInternalScanRange*> res;
    if (_assign_morsels) {
        for (auto& [_, morsels] : _morsels_per_operator) {
            _convert_morsels_to_olap_scan_ranges(morsels, &res);
        }
    } else {
        _convert_morsels_to_olap_scan_ranges(_morsels, &res);
    }
    return res;
}

FixedMorselQueue::FixedMorselQueue(Morsels&& morsels, int dop)
        : _morsels(std::move(morsels)), _num_morsels(_morsels.size()), _pop_index(0), _scan_dop(dop) {
    int io_parallelism = dop * ScanOperator::MAX_IO_TASKS_PER_OP;
    if (dop > 1 && _num_morsels <= io_parallelism) {
        for (int i = 0; i < dop; i++) {
            _next_morsel_index_per_operator[i] = 0;
        }
        int operator_seq = 0;
        for (int i = 0; i < _morsels.size(); i++) {
            _morsels_per_operator[operator_seq].push_back(std::move(_morsels[i]));
            operator_seq = (operator_seq + 1) % _scan_dop;
        }

        _morsels.clear();
        _assign_morsels = true;
    } else {
        _assign_morsels = false;
    }
}

bool FixedMorselQueue::empty() const {
    if (_assign_morsels) {
        for (auto& [seq, index] : _next_morsel_index_per_operator) {
            if (index.load() < _morsels_per_operator.at(seq).size()) {
                return false;
            }
        }
        return true;
    } else {
        return _pop_index >= _num_morsels;
    }
}

StatusOr<MorselPtr> FixedMorselQueue::try_get(int driver_seq) {
    if (_assign_morsels) {
        auto& next_index = _next_morsel_index_per_operator[driver_seq];
        auto& morsels = _morsels_per_operator[driver_seq];
        int idx = next_index.load();
        if (idx >= morsels.size()) {
            return nullptr;
        }
        idx = next_index.fetch_add(1);
        if (idx >= morsels.size()) {
            return nullptr;
        }
        return std::move(morsels[idx]);
    } else {
        auto idx = _pop_index.load();
        // prevent _num_morsels from superfluous addition
        if (idx >= _num_morsels) {
            return nullptr;
        }
        idx = _pop_index.fetch_add(1);
        if (idx < _num_morsels) {
            return std::move(_morsels[idx]);
        } else {
            return nullptr;
        }
    }
}

std::vector<TInternalScanRange*> PhysicalSplitMorselQueue::olap_scan_ranges() const {
    std::vector<TInternalScanRange*> res;
    _convert_morsels_to_olap_scan_ranges(_morsels, &res);
    return res;
}

void PhysicalSplitMorselQueue::set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) {
    for (const auto& key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _range_start_op = key_range->begin_include ? "ge" : "gt";
        _range_end_op = key_range->end_include ? "le" : "lt";

        _range_start_key.emplace_back(key_range->begin_scan_range);
        _range_end_key.emplace_back(key_range->end_scan_range);
    }
}

StatusOr<MorselPtr> PhysicalSplitMorselQueue::try_get(int driver_seq) {
    DCHECK(!_tablets.empty());
    DCHECK(!_tablet_rowsets.empty());
    DCHECK_EQ(_tablets.size(), _tablet_rowsets.size());

    std::lock_guard<std::mutex> lock(_mutex);

    if (_tablet_idx >= _tablets.size()) {
        return nullptr;
    }

    // When it hasn't initialized any segment,
    // or _segment_idx exceeds the segments of the current rowset,
    // or current segment is empty or finished,
    // we should pick up the next segment and init it.
    while (!_has_init_any_segment || _cur_segment() == nullptr || _cur_segment()->num_rows() == 0 ||
           !_segment_range_iter.has_more()) {
        if (!_next_segment()) {
            return nullptr;
        }

        if (auto status = _init_segment(); !status.ok()) {
            // Morsel_queue cannot generate morsels after errors occurring.
            _tablet_idx = _tablets.size();
            return status;
        }
    }

    vectorized::SparseRange taken_range;
    _segment_range_iter.next_range(_splitted_scan_rows, &taken_range);
    _num_segment_rest_rows -= taken_range.span_size();
    if (_num_segment_rest_rows < _splitted_scan_rows) {
        // If there are too few rows left in the segment, take them all this time.
        _segment_range_iter.next_range(_splitted_scan_rows, &taken_range);
        _num_segment_rest_rows = 0;
    }

    auto* scan_morsel = _cur_scan_morsel();
    auto* rowset = _cur_rowset();
    auto rowid_range = std::make_shared<vectorized::RowidRangeOption>(
            rowset->rowset_id(), rowset->segments()[_segment_idx]->id(), std::move(taken_range));
    MorselPtr morsel = std::make_unique<PhysicalSplitScanMorsel>(
            scan_morsel->get_plan_node_id(), *(scan_morsel->get_scan_range()), std::move(rowid_range));

    return morsel;
}

rowid_t PhysicalSplitMorselQueue::_lower_bound_ordinal(Segment* segment, const vectorized::SeekTuple& key, bool lower) {
    std::string index_key =
            key.short_key_encode(segment->num_short_keys(), lower ? KEY_MINIMAL_MARKER : KEY_MAXIMAL_MARKER);
    uint32_t start_block_id;
    auto start_iter = segment->lower_bound(index_key);
    if (start_iter.valid()) {
        // Because previous block may contain this key, so we should set rowid to
        // last block's first row.
        start_block_id = start_iter.ordinal();
        if (start_block_id > 0) {
            start_block_id--;
        }
    } else {
        // When we don't find a valid index item, which means all short key is
        // smaller than input key, this means that this key may exist in the last
        // row block. so we set the rowid to first row of last row block.
        start_block_id = segment->last_block();
    }

    return start_block_id * segment->num_rows_per_block();
}

rowid_t PhysicalSplitMorselQueue::_upper_bound_ordinal(Segment* segment, const vectorized::SeekTuple& key, bool lower,
                                                       rowid_t end) {
    std::string index_key =
            key.short_key_encode(segment->num_short_keys(), lower ? KEY_MINIMAL_MARKER : KEY_MAXIMAL_MARKER);

    auto end_iter = segment->upper_bound(index_key);
    if (end_iter.valid()) {
        end = end_iter.ordinal() * segment->num_rows_per_block();
    }

    return end;
}

ScanMorsel* PhysicalSplitMorselQueue::_cur_scan_morsel() {
    return down_cast<ScanMorsel*>(_morsels[_tablet_idx].get());
}

BetaRowset* PhysicalSplitMorselQueue::_cur_rowset() {
    return down_cast<BetaRowset*>(_tablet_rowsets[_tablet_idx][_rowset_idx].get());
}

Segment* PhysicalSplitMorselQueue::_cur_segment() {
    const auto& segments = _cur_rowset()->segments();
    return _segment_idx >= segments.size() ? nullptr : segments[_segment_idx].get();
}

bool PhysicalSplitMorselQueue::_next_segment() {
    DCHECK(_num_segment_rest_rows == 0);
    if (!_has_init_any_segment) {
        _has_init_any_segment = true;
    } else {
        // Read the next segment of the current rowset.
        if (++_segment_idx >= _cur_rowset()->segments().size()) {
            _segment_idx = 0;
            // Read the next rowset of the current tablet.
            if (++_rowset_idx >= _tablet_rowsets[_tablet_idx].size()) {
                _rowset_idx = 0;
                // Read the next tablet.
                ++_tablet_idx;
            }
        }
    }

    return _tablet_idx < _tablets.size();
}

Status PhysicalSplitMorselQueue::_init_segment() {
    // Load the meta of the new rowset and the index of the new segment。
    if (0 == _segment_idx) {
        // Read a new tablet.
        if (0 == _rowset_idx) {
            _tablet_seek_ranges.clear();
            _mempool.clear();
            RETURN_IF_ERROR(vectorized::TabletReader::parse_seek_range(_tablets[_tablet_idx], _range_start_op,
                                                                       _range_end_op, _range_start_key, _range_end_key,
                                                                       &_tablet_seek_ranges, &_mempool));
        }
        // Read a new rowset.
        RETURN_IF_ERROR(_cur_rowset()->load());
    }

    _num_segment_rest_rows = 0;
    _segment_scan_range.clear();

    auto* segment = _cur_segment();
    // The new rowset doesn't contain any segment.
    if (segment == nullptr || segment->num_rows() == 0) {
        return Status::OK();
    }

    // Find the rowid range of each key range in this segment.
    if (_tablet_seek_ranges.empty()) {
        _segment_scan_range.add(vectorized::Range(0, segment->num_rows()));
    } else {
        RETURN_IF_ERROR(segment->load_index(StorageEngine::instance()->metadata_mem_tracker()));
        for (const auto& range : _tablet_seek_ranges) {
            rowid_t lower_rowid = 0;
            rowid_t upper_rowid = segment->num_rows();

            if (!range.upper().empty()) {
                upper_rowid =
                        _upper_bound_ordinal(segment, range.upper(), !range.inclusive_upper(), segment->num_rows());
            }
            if (!range.lower().empty() && upper_rowid > 0) {
                lower_rowid = _lower_bound_ordinal(segment, range.lower(), range.inclusive_lower());
            }
            if (lower_rowid <= upper_rowid) {
                _segment_scan_range.add(vectorized::Range{lower_rowid, upper_rowid});
            }
        }
    }

    _segment_range_iter = _segment_scan_range.new_iterator();
    _num_segment_rest_rows = _segment_scan_range.span_size();

    return Status::OK();
}

} // namespace pipeline
} // namespace starrocks
