// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "exec/pipeline/scan/morsel.h"

#include "exec/olap_utils.h"
#include "storage/chunk_helper.h"
#include "storage/range.h"
#include "storage/rowset/rowid_range_option.h"
#include "storage/rowset/rowset.h"
#include "storage/rowset/short_key_range_option.h"
#include "storage/storage_engine.h"
#include "storage/tablet_reader.h"
#include "storage/tablet_reader_params.h"

namespace starrocks {
namespace pipeline {

/// Morsel.
void PhysicalSplitScanMorsel::init_tablet_reader_params(vectorized::TabletReaderParams* params) {
    params->rowid_range_option = _rowid_range_option;
}

void LogicalSplitScanMorsel::init_tablet_reader_params(vectorized::TabletReaderParams* params) {
    params->short_key_ranges = _short_key_ranges;
}

/// MorselQueueFactory.
size_t SharedMorselQueueFactory::num_original_morsels() const {
    return _queue->num_original_morsels();
}

size_t IndividualMorselQueueFactory::num_original_morsels() const {
    size_t total = 0;
    for (const auto& queue : _queue_per_driver_seq) {
        total += queue->num_original_morsels();
    }
    return total;
}

IndividualMorselQueueFactory::IndividualMorselQueueFactory(std::map<int, MorselQueuePtr>&& queue_per_driver_seq,
                                                           bool need_local_shuffle)
        : _need_local_shuffle(need_local_shuffle) {
    if (queue_per_driver_seq.empty()) {
        _queue_per_driver_seq.emplace_back(pipeline::create_empty_morsel_queue());
        return;
    }

    int max_dop = queue_per_driver_seq.rbegin()->first;
    _queue_per_driver_seq.reserve(max_dop + 1);
    for (int i = 0; i <= max_dop; ++i) {
        auto it = queue_per_driver_seq.find(i);
        if (it == queue_per_driver_seq.end()) {
            _queue_per_driver_seq.emplace_back(create_empty_morsel_queue());
        } else {
            _queue_per_driver_seq.emplace_back(std::move(it->second));
        }
    }
}

/// MorselQueue.
std::vector<TInternalScanRange*> _convert_morsels_to_olap_scan_ranges(const Morsels& morsels) {
    std::vector<TInternalScanRange*> scan_ranges;
    scan_ranges.reserve(morsels.size());
    for (const auto& morsel : morsels) {
        auto* scan_morsel = down_cast<ScanMorsel*>(morsel.get());
        auto* scan_range = scan_morsel->get_olap_scan_range();
        scan_ranges.emplace_back(scan_range);
    }
    return scan_ranges;
}

std::vector<TInternalScanRange*> FixedMorselQueue::olap_scan_ranges() const {
    return _convert_morsels_to_olap_scan_ranges(_morsels);
}

StatusOr<MorselPtr> FixedMorselQueue::try_get() {
    auto idx = _pop_index.load();
    // prevent _num_morsels from superfluous addition
    if (idx >= _num_morsels) {
        return nullptr;
    }
    idx = _pop_index.fetch_add(1);
    if (idx < _num_morsels) {
        if (!_tablet_rowsets.empty()) {
            _morsels[idx]->set_rowsets(std::move(_tablet_rowsets[idx]));
        }
        return std::move(_morsels[idx]);
    } else {
        return nullptr;
    }
}

std::vector<TInternalScanRange*> PhysicalSplitMorselQueue::olap_scan_ranges() const {
    return _convert_morsels_to_olap_scan_ranges(_morsels);
}

void PhysicalSplitMorselQueue::set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) {
    for (const auto& key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _range_start_op = key_range->begin_include ? vectorized::TabletReaderParams::RangeStartOperation::GE
                                                   : vectorized::TabletReaderParams::RangeStartOperation::GT;
        _range_end_op = key_range->end_include ? vectorized::TabletReaderParams::RangeEndOperation::LE
                                               : vectorized::TabletReaderParams::RangeEndOperation::LT;

        _range_start_key.emplace_back(key_range->begin_scan_range);
        _range_end_key.emplace_back(key_range->end_scan_range);
    }
}

StatusOr<MorselPtr> PhysicalSplitMorselQueue::try_get() {
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
    morsel->set_rowsets(_tablet_rowsets[_tablet_idx]);
    return morsel;
}

rowid_t PhysicalSplitMorselQueue::_lower_bound_ordinal(Segment* segment, const vectorized::SeekTuple& key,
                                                       bool lower) const {
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
                                                       rowid_t end) const {
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

Rowset* PhysicalSplitMorselQueue::_cur_rowset() {
    return _tablet_rowsets[_tablet_idx][_rowset_idx].get();
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
        RETURN_IF_ERROR(segment->load_index());
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

std::vector<TInternalScanRange*> LogicalSplitMorselQueue::olap_scan_ranges() const {
    return _convert_morsels_to_olap_scan_ranges(_morsels);
}

void LogicalSplitMorselQueue::set_key_ranges(const std::vector<std::unique_ptr<OlapScanRange>>& key_ranges) {
    for (const auto& key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 && key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _range_start_op = key_range->begin_include ? vectorized::TabletReaderParams::RangeStartOperation::GE
                                                   : vectorized::TabletReaderParams::RangeStartOperation::GT;
        _range_end_op = key_range->end_include ? vectorized::TabletReaderParams::RangeEndOperation::LE
                                               : vectorized::TabletReaderParams::RangeEndOperation::LT;

        _range_start_key.emplace_back(key_range->begin_scan_range);
        _range_end_key.emplace_back(key_range->end_scan_range);
    }
}

StatusOr<MorselPtr> LogicalSplitMorselQueue::try_get() {
    DCHECK(!_tablets.empty());
    DCHECK(!_tablet_rowsets.empty());
    DCHECK_EQ(_tablets.size(), _tablet_rowsets.size());

    std::lock_guard<std::mutex> lock(_mutex);

    if (_tablet_idx >= _tablets.size()) {
        return nullptr;
    }

    // When it hasn't initialized any tablet,
    // or the current tablet doesn't contain any segment,
    // or all the key ranges of the current tablet has been finished,
    // we should pick up the next tablet and init it.
    while (!_has_init_any_tablet || _segment_group == nullptr || _cur_tablet_finished()) {
        if (!_next_tablet()) {
            return nullptr;
        }
        RETURN_IF_ERROR(_init_tablet());
    }

    // Take sub key ranges from each key range, until the number of taken blocks is greater than
    // `_sample_splitted_scan_blocks`.
    //
    // As for the current key range, try to use the next `STEP`-th short key as the upper point of
    // the sub key range. The upper point must be different from the lower point.
    // Therefore, if it is the same as the lower point, try the next `STEP`-th short key repeatedly.
    //
    // About `STEP`:
    // - `STEP` is equal to `_sample_splitted_scan_blocks-num_taken_blocks` for the most cases,
    //   where `num_taken_blocks` is the number of already taken blocks fot the current morsel.
    // - If the number of taken blocks is greater than `_sample_splitted_scan_blocks-num_taken_blocks` but the
    //   upper point different from the lower point hasn't been found, `STEP` is `_sample_splitted_scan_blocks/4`
    //   to avoid generating a too large morsel.
    // - As for the last key range, if the number of the rest blocks is slightly greater than `_sample_splitted_scan_blocks`,
    //   there will be too little blocks after this time taking. Therefore, this time taking and the next time taking share
    //   the rest blocks equally.
    //
    // For example, assume that _sample_splitted_scan_blocks=3, 3 key ranges with the following short keys:
    //   index:      0  1  2  3  4  5  6
    // - key range1: 11 12 13 14 15
    // - key range2: 21 22 22 22 22 24 25
    // - key range3: 31 32 33 34 35 36
    // Then, it will generate the following 6 morsels:
    // - morsel1: key range1 [11, 14).
    // - morsel2: key range1 [14, 15], key range2 [21, 22).
    // - morsel3: key range2 [22, 24).
    // - morsel4: key range2 [24, 25], key range3 [31, 32).
    // - morsel5: key range3 [33, 35).
    // - morsel6: key range3 [35, 36].
    //
    // As for morsel3, lower index is 1, and try to use index 4 as upper index firstly.
    // The short keys of index 1 and 4 are both 22, so use index 5 as the range upper.
    // As for morsel5, it trys to take index 2~4 firstly, but there will be only 1 block left.
    // Therefore, morsel5 and morsel6 each takes 2 morsel.
    size_t num_taken_blocks = 0;
    std::vector<vectorized::ShortKeyRangeOptionPtr> short_key_ranges;
    vectorized::ShortKeyOptionPtr _cur_range_lower = nullptr;
    vectorized::ShortKeyOptionPtr _cur_range_upper = nullptr;
    bool need_more_blocks = true;
    while (!_cur_tablet_finished() &&      // One morsel only read data from one tablet.
           (_cur_range_lower != nullptr || // Haven't found the _cur_range_upper different from _cur_range_lower.
            (need_more_blocks && num_taken_blocks < _sample_splitted_scan_blocks))) {
        auto& num_rest_blocks = _num_rest_blocks_per_seek_range[_range_idx];

        if (_cur_range_lower == nullptr) {
            _cur_range_lower = _create_range_lower();
        }

        size_t cur_num_taken_blocks = 0;
        if (num_taken_blocks < _sample_splitted_scan_blocks) {
            cur_num_taken_blocks = _sample_splitted_scan_blocks - num_taken_blocks;
        } else {
            // If it has taken enough blocks but hasn't found the _cur_range_upper different from _cur_range_lower,
            // just take quarter of _sample_splitted_scan_blocks once.
            cur_num_taken_blocks = std::max<int64_t>(_sample_splitted_scan_blocks / 4, 1);
        }
        cur_num_taken_blocks = std::min(cur_num_taken_blocks, num_rest_blocks);

        // As for the last key range, if there is not enough blocks after this time taking,
        // take from the last key range at most half of the rest blocks.
        if (_range_idx + 1 >= _block_ranges_per_seek_range.size() && num_rest_blocks > cur_num_taken_blocks &&
            num_rest_blocks - cur_num_taken_blocks < _sample_splitted_scan_blocks) {
            cur_num_taken_blocks = std::min(cur_num_taken_blocks, num_rest_blocks / 2);
            need_more_blocks = false;
        }

        num_rest_blocks -= cur_num_taken_blocks;
        num_taken_blocks += cur_num_taken_blocks;
        _next_lower_block_iter += static_cast<ssize_t>(cur_num_taken_blocks);

        _cur_range_upper = _create_range_upper();

        if (num_rest_blocks == 0 || _valid_range(_cur_range_lower, _cur_range_upper)) {
            short_key_ranges.emplace_back(std::make_shared<vectorized::ShortKeyRangeOption>(
                    std::move(_cur_range_lower), std::move(_cur_range_upper)));
        }

        // The current key range has no more blocks, so move to next key range.
        if (num_rest_blocks == 0) {
            ++_range_idx;
            if (!_cur_tablet_finished()) {
                _next_lower_block_iter = _block_ranges_per_seek_range[_range_idx].first;
            }
        }
    }
    DCHECK(_cur_range_lower == nullptr);
    DCHECK(_cur_range_upper == nullptr);

    auto* scan_morsel = down_cast<ScanMorsel*>(_morsels[_tablet_idx].get());
    auto morsel = std::make_unique<LogicalSplitScanMorsel>(
            scan_morsel->get_plan_node_id(), *(scan_morsel->get_scan_range()), std::move(short_key_ranges));
    morsel->set_rowsets(_tablet_rowsets[_tablet_idx]);
    return morsel;
}

// Validate that the splitted start short key and end short key shouldn't be the same.
bool LogicalSplitMorselQueue::_valid_range(const vectorized::ShortKeyOptionPtr& lower,
                                           const vectorized::ShortKeyOptionPtr& upper) const {
    // It is validated that any of endpoint is infinite or the original short key range is a point.
    if (lower->is_infinite() || upper->is_infinite() ||
        _block_ranges_per_seek_range[_range_idx].first == _block_ranges_per_seek_range[_range_idx].second) {
        return true;
    }

    Slice lower_key = lower->short_key;
    // Empty short key of start ShortKeyOption means it is the first splitted key range,
    // so use start original short key to compare.
    if (lower_key.empty()) {
        lower_key = *_block_ranges_per_seek_range[_range_idx].first;
    }

    Slice upper_key = upper->short_key;
    // Empty short key of end ShortKeyOption means it is the last splitted key range,
    // so use end original short key to compare.
    if (upper_key.empty()) {
        auto end_iter = _block_ranges_per_seek_range[_range_idx].second;
        --end_iter;
        upper_key = *end_iter;
    }

    return lower_key.compare(upper_key) != 0;
}

vectorized::ShortKeyOptionPtr LogicalSplitMorselQueue::_create_range_lower() const {
    // If it is the first splitted key range, the start point is the original start key.
    if (_next_lower_block_iter == _block_ranges_per_seek_range[_range_idx].first) {
        if (_tablet_seek_ranges.empty()) {
            return std::make_unique<vectorized::ShortKeyOption>();
        } else {
            return std::make_unique<vectorized::ShortKeyOption>(&_tablet_seek_ranges[_range_idx].lower(),
                                                                _tablet_seek_ranges[_range_idx].inclusive_lower());
        }
    } else {
        Slice short_key = *_next_lower_block_iter;
        return std::make_unique<vectorized::ShortKeyOption>(_short_key_schema, short_key, true);
    }
}

vectorized::ShortKeyOptionPtr LogicalSplitMorselQueue::_create_range_upper() const {
    // If it is the last splitted key range, the end point is the original end key.
    if (_next_lower_block_iter == _block_ranges_per_seek_range[_range_idx].second) {
        if (_tablet_seek_ranges.empty()) {
            return std::make_unique<vectorized::ShortKeyOption>();
        } else {
            return std::make_unique<vectorized::ShortKeyOption>(&_tablet_seek_ranges[_range_idx].upper(),
                                                                _tablet_seek_ranges[_range_idx].inclusive_upper());
        }
    } else {
        Slice short_key = *_next_lower_block_iter;
        return std::make_unique<vectorized::ShortKeyOption>(_short_key_schema, short_key, false);
    }
}

bool LogicalSplitMorselQueue::_cur_tablet_finished() const {
    return _range_idx >= _block_ranges_per_seek_range.size();
}

Rowset* LogicalSplitMorselQueue::_find_largest_rowset(const std::vector<RowsetSharedPtr>& rowsets) {
    if (rowsets.empty()) {
        return nullptr;
    }

    Rowset* largest_rowset = rowsets[0].get();
    for (int i = 1; i < rowsets.size(); ++i) {
        if (largest_rowset->num_rows() < rowsets[i]->num_rows()) {
            largest_rowset = rowsets[i].get();
        }
    }

    return largest_rowset;
}

SegmentSharedPtr LogicalSplitMorselQueue::_find_largest_segment(Rowset* rowset) const {
    const auto& segments = rowset->segments();
    if (segments.empty()) {
        return nullptr;
    }

    SegmentSharedPtr largest_segment = segments[0];
    for (int i = 1; i < segments.size(); ++i) {
        if (largest_segment->num_rows() < segments[i]->num_rows()) {
            largest_segment = segments[i];
        }
    }

    return largest_segment;
}

StatusOr<SegmentGroupPtr> LogicalSplitMorselQueue::_create_segment_group(Rowset* rowset) {
    std::vector<SegmentSharedPtr> segments;
    if (rowset->rowset_meta()->is_segments_overlapping()) {
        segments.emplace_back(_find_largest_segment(rowset));
    } else {
        segments = rowset->segments();
    }

    for (const auto& segment : segments) {
        RETURN_IF_ERROR(segment->load_index());
    }

    return std::make_unique<SegmentGroup>(std::move(segments));
}

bool LogicalSplitMorselQueue::_next_tablet() {
    if (!_has_init_any_tablet) {
        _has_init_any_tablet = true;
    } else {
        ++_tablet_idx;
    }

    return _tablet_idx < _tablets.size();
}

Status LogicalSplitMorselQueue::_init_tablet() {
    _largest_rowset = nullptr;
    _segment_group = nullptr;
    _short_key_schema = nullptr;
    _block_ranges_per_seek_range.clear();
    _num_rest_blocks_per_seek_range.clear();
    _range_idx = 0;

    if (_tablet_idx == 0) {
        // All the tablets have the same schema, so parse seek range with the first table schema.
        RETURN_IF_ERROR(vectorized::TabletReader::parse_seek_range(_tablets[_tablet_idx], _range_start_op,
                                                                   _range_end_op, _range_start_key, _range_end_key,
                                                                   &_tablet_seek_ranges, &_mempool));
    }

    _largest_rowset = _find_largest_rowset(_tablet_rowsets[_tablet_idx]);
    if (_largest_rowset == nullptr || _largest_rowset->num_rows() == 0) {
        return Status::OK();
    }

    RETURN_IF_ERROR(_largest_rowset->load());
    ASSIGN_OR_RETURN(_segment_group, _create_segment_group(_largest_rowset));

    _short_key_schema = std::make_shared<vectorized::Schema>(
            ChunkHelper::get_short_key_schema_with_format_v2(_tablets[_tablet_idx]->tablet_schema()));
    _sample_splitted_scan_blocks =
            _splitted_scan_rows * _segment_group->num_blocks() / _tablets[_tablet_idx]->num_rows();
    _sample_splitted_scan_blocks = std::max<int64_t>(_sample_splitted_scan_blocks, 1);

    if (_tablet_seek_ranges.empty()) {
        _block_ranges_per_seek_range.emplace_back(_segment_group->begin(), _segment_group->end());
        _num_rest_blocks_per_seek_range.emplace_back(_segment_group->num_blocks());
    } else {
        for (const auto& range : _tablet_seek_ranges) {
            ShortKeyIndexGroupIterator upper_block_iter;
            if (!range.upper().empty()) {
                upper_block_iter = _upper_bound_ordinal(range.upper(), !range.inclusive_upper());
            } else {
                upper_block_iter = _segment_group->end();
            }

            ShortKeyIndexGroupIterator lower_block_iter;
            if (!range.lower().empty() && upper_block_iter.ordinal() > 0) {
                lower_block_iter = _lower_bound_ordinal(range.lower(), range.inclusive_lower());
            } else {
                lower_block_iter = _segment_group->begin();
            }

            _num_rest_blocks_per_seek_range.emplace_back(upper_block_iter - lower_block_iter);
            _block_ranges_per_seek_range.emplace_back(lower_block_iter, upper_block_iter);
        }
    }
    _next_lower_block_iter = _block_ranges_per_seek_range[0].first;

    return Status::OK();
}

ShortKeyIndexGroupIterator LogicalSplitMorselQueue::_lower_bound_ordinal(const vectorized::SeekTuple& key,
                                                                         bool lower) const {
    std::string index_key =
            key.short_key_encode(_segment_group->num_short_keys(), lower ? KEY_MINIMAL_MARKER : KEY_MAXIMAL_MARKER);

    auto start_iter = _segment_group->lower_bound(index_key);
    if (start_iter.valid()) {
        // Because previous block may contain this key, so we should set rowid to
        // last block's first row.
        if (start_iter.ordinal() > 0) {
            --start_iter;
        }
    } else {
        // When we don't find a valid index item, which means all short key is
        // smaller than input key, this means that this key may exist in the last
        // row block. so we set the rowid to first row of last row block.
        start_iter = _segment_group->back();
    }

    return start_iter;
}

ShortKeyIndexGroupIterator LogicalSplitMorselQueue::_upper_bound_ordinal(const vectorized::SeekTuple& key,
                                                                         bool lower) const {
    std::string index_key =
            key.short_key_encode(_segment_group->num_short_keys(), lower ? KEY_MINIMAL_MARKER : KEY_MAXIMAL_MARKER);

    auto end_iter = _segment_group->upper_bound(index_key);
    return end_iter;
}

MorselQueuePtr create_empty_morsel_queue() {
    return std::make_unique<FixedMorselQueue>(std::vector<MorselPtr>{});
}

} // namespace pipeline
} // namespace starrocks
