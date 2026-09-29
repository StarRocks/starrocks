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

#include <cstddef>
#include <cstdint>
#include <memory>
#include <vector>

#include "common/statusor.h"
#include "storage/variant_tuple.h"
#include "types/logical_type.h"

namespace starrocks {

class Schema;
class Segment;
class TabletSchema;
using TabletSchemaCSPtr = std::shared_ptr<const TabletSchema>;

// Per-split sample resolution. Mirrors kSamplesPerSplit in
// get_tablet_split_ranges_from_pk_index_impl (storage/lake/tablet_splitter.cpp) so the two budget
// schemes in that file agree.
constexpr int64_t kSortKeySamplesPerSplit = 32;

// Number of bytes SeekTuple::short_key_encode appends for a value of |lt|, or 0 when a short key
// index entry holding |lt| cannot be decoded back to the original value.
//
// Switches on delegate_type(lt), mirroring get_key_coder, so this width cannot drift from the
// coder that actually produced the bytes.
//
// Returns 0 for CHAR (pads to index_size), VARCHAR/VARBINARY (truncate), and INT256/DECIMAL256
// (their short-key encoder is deliberately empty -- KeyCoderTraits<TYPE_INT256>::encode_ascending,
// inherited by KeyCoderTraits<TYPE_DECIMAL256>).
size_t sort_key_fixed_encode_size(LogicalType lt);

// True iff |schema|'s legacy short key index entries are byte-identical to a full, untruncated
// order-preserving encoding of the whole sort key -- i.e. the short key spans every sort key column
// and every one of them is a fixed-size type this module can decode.
//
// |num_short_key_columns| and |sort_key_idxes| must come from the schema the SEGMENT was written
// with, never from the tablet's current schema.
bool short_key_index_encodes_full_sort_key(const Schema& schema, const std::vector<uint32_t>& sort_key_idxes,
                                           size_t num_short_key_columns);

// Decode one short key index entry into typed values, one per requested sort key column.
//
// Only the fixed-size types sort_key_fixed_encode_size accepts are handled -- which is exactly what
// short_key_index_encodes_full_sort_key admits, so a CHAR/VARCHAR sort key never reaches here and
// there is deliberately no variable-length branch. Each column is a marker byte
// (KEY_NULL_FIRST_MARKER for NULL, otherwise KEY_NORMAL_MARKER) followed, when non-NULL, by the key
// coder's fixed-width encoding. The entry carries no trailing padding: the writer only appends one
// when chunk.num_columns() < num_short_key_columns, which no writer pass produces.
//
// The decoded tuple owns its values -- every fixed decode_ascending writes into a stack CppType and
// Datum copies by value, so nothing points into the scratch MemPool.
Status decode_short_key_entry(const Slice& encoded, const Schema& schema, const std::vector<uint32_t>& sort_key_idxes,
                              VariantTuple* out);

// Equal-row-interval sort key samples. samples[i] is the key at 0-indexed row (i+1) * row_interval.
// Invariants distribute_segment_to_ranges (storage/lake/tablet_splitter.cpp) depends on:
//   * samples.empty() <=> row_interval == 0
//   * samples.size() * row_interval < num_rows
// The consumer assigns EXACTLY row_interval rows to each of the first samples.size() sub-segments,
// so the spacing must be uniform. Violating either invariant is silent corruption in a release
// build, not an error: DCHECKs compile out and a negative row delta poisons the whole estimate.
struct SortKeySamples {
    std::vector<VariantTuple> samples;
    int64_t row_interval = 0;
};

// Sample the sort key from |segment|'s legacy short key index page. No data-page I/O: the caller
// must have called segment.load_index(), and the short key page it parsed is the only thing read.
//
// Precondition: short_key_index_encodes_full_sort_key() is true for the schema |segment| was
// written with, so its entries decode as a full order-preserving sort key. |schema| and
// |sort_key_idxes| must be that same historical schema's.
//
// Takes every stride-th entry so the spacing stays uniform (the consumer assigns exactly
// row_interval rows per sub-segment), and reports row_interval = stride * num_rows_per_block().
//
// |num_rows| is the row count the CONSUMER will use (SegmentMetadataPB.num_rows), which is a
// different source from the index page's own geometry and can disagree with it. It is required so
// the geometry identity and the carrier invariant can be verified rather than assumed:
// num_segment_rows() must equal it and num_items must equal ceil(num_rows / num_rows_per_block()).
//
// Returns EMPTY samples -- never an error -- whenever this index cannot produce trustworthy ones:
// unusable or inconsistent page geometry, a decoded entry 0 that is not sort_key_min, an entry that
// fails to decode, a sample that breaks arity/monotonicity/bounds, a carrier that would not fit
// |num_rows|, or simply a segment of fewer than two blocks. In every one of those cases the caller
// falls through to sample_sort_key_from_segment_data, which can sample at row rather than block
// granularity. Never fails the split.
StatusOr<SortKeySamples> sample_sort_key_from_short_key_index(Segment& segment, const Schema& schema,
                                                              const std::vector<uint32_t>& sort_key_idxes,
                                                              const VariantTuple& min_key, const VariantTuple& max_key,
                                                              int64_t num_rows, int64_t target);

// Sample the sort key by reading |target| rows out of |segment|'s data pages, at a uniform row
// stride. Used when the short key index does not encode the whole sort key.
//
// Costs #samples * #sort_key_columns page reads. The sampled ordinals ascend within each column, so
// each column is read as a strided forward scan rather than random access.
//
// |segment_schema| is the schema to READ |segment| with: either the schema |segment| was written
// with, or a newer one whose extra trailing key columns are then correctly detected as absent (this
// is a supported call -- see path_b_refuses_when_a_sort_key_column_is_missing). |fill_data_cache|
// should be false for tablet split (it never reads the data again, and sparse pages would evict
// useful entries) and true for range-split compaction (which is about to read all of it).
//
// Returns empty samples -- never an error -- when there is nothing to sample (target <= 0,
// num_rows <= row_interval), when |sort_key_idxes| does not match |segment_schema|'s own sort key
// column ids, when a sort key column is absent from this segment, or when any read or validation
// fails. The caller then uses the coarse [min_key, max_key] range. Never fails the split.
StatusOr<SortKeySamples> sample_sort_key_from_segment_data(Segment& segment, const TabletSchemaCSPtr& segment_schema,
                                                           const std::vector<uint32_t>& sort_key_idxes,
                                                           int64_t num_rows, const VariantTuple& min_key,
                                                           const VariantTuple& max_key, int64_t target,
                                                           bool fill_data_cache);

// Per-segment sample budget for one tablet, parallel to |segment_num_rows|.
//
// The tablet target is min(config::sort_key_max_samples_per_tablet, kSortKeySamplesPerSplit *
// split_width) -- scaled by the requested width so a 2-way split does not pay for a 1024-way one --
// and each segment's share is that target apportioned by row count.
//
// There is deliberately NO per-segment ceiling: the tablet target already bounds the total (integer
// division only loses remainder, so sum(share) <= target), so a per-segment policy on top adds
// nothing but a second place to get the arithmetic wrong -- which is exactly what happened, since
// the ceiling and the proportional share disagreed about the ten-equal-segments case.
//
// (An earlier note here claimed the ceiling starved a 1024-way single-segment split. That was
// arithmetically false -- max(32, 1024) is 1024 -- and is recorded only so the wrong reason is not
// reintroduced as a justification for putting a ceiling back.)
//
// |split_width| is the caller's requested output width -- split_count for tablet split, or
// max(2, max_parallel) for range-split compaction. Note max_parallel ALONE is not an upper bound on
// that path's target_subtasks, because target_subtasks = max(2, min(max_parallel, ...)).
//
// Pre-computing the whole vector, rather than decrementing a running budget inside the rowset loop,
// is what makes the distribution proportional instead of first-come-first-served.
//
// Shares are apportioned by LARGEST REMAINDER, so the residual left by flooring is handed out
// rather than discarded, and sum(budget) == min(target_total, positive segment count). Flooring each
// share independently loses the entire budget once a tablet has more segments than samples -- at the
// default cap a 2-way split targets 64, so 65 equal segments each floor to zero and the tablet is
// not sampled at all, which is the opposite of what a fragmented tablet needs.
//
// A budget of 0 for a segment is legal and means "use the coarse [min, max] range"; a cap of 0
// disables sampling for the whole tablet. Producing exactly split_width ranges is best-effort: the
// candidate set is 2*segments + sum(budget), and calculate_range_split_boundaries returns fewer
// boundaries when it cannot do better.
std::vector<int64_t> allocate_sort_key_sample_budget(const std::vector<int64_t>& segment_num_rows, int64_t split_width);

// Read accessors for two of this module's bvars, so a caller's test can assert which PATH ran
// rather than only what came out of it -- both paths publish their samples through the same
// SortKeySamples carrier, so the carrier alone cannot tell them apart.
//
// sort_key_sampling_data_page_segments_count() counts segments for which this module COMMITTED to
// the data-page path: incremented once the sampling geometry is fixed and before the first read, so
// a segment whose read then fails open is counted too. That is deliberate -- the counter's job is to
// reveal that path A was not taken, and a fall-through that then fails to read matters just as much
// as one that succeeds. For the I/O actually paid, read sort_key_sampling_read_bytes_total, which
// accrues only what the page reads transferred. A caller that expects the free short-key-index path
// asserts this does not move; that is the only direct evidence path A ran rather than path B quietly
// doing the work.
//
// sort_key_sampling_samples_count() counts every sample published, by either path. An increase here
// with no increase above means path A produced them.
//
// Both are process-global monotonic counters, so read them as a before/after DELTA, never as an
// absolute.
int64_t sort_key_sampling_data_page_segments_count();
int64_t sort_key_sampling_samples_count();

// Read accessor for path B's fallback-rejection counter, so a caller's test can assert a rejection
// was counted without going through bvar's string-keyed exposition (which silently returns a
// meaningless value on a typo'd name instead of failing to compile). A process-global monotonic
// counter, so read it as a before/after DELTA.
int64_t sort_key_sampling_data_page_fallback_count();

// Read accessor for path A's latency recorder's sample count, so a caller's test can assert a
// no-op early return (e.g. a zero budget) does NOT charge a sample into the histogram -- otherwise
// many no-op segments would depress its percentiles with zero-duration entries. A process-global
// monotonic counter, so read it as a before/after DELTA.
int64_t sort_key_sampling_short_key_index_latency_count();

// Records that a caller opened one rowset's segment files in order to sample them. Called by the
// CALLER, because that open happens before any function in this module is entered, and it is the
// only I/O on this path that nothing else here can see: the counters above start at a segment that
// is already open. Together with them it also makes a budget that never reaches the segments being
// compacted visible -- rowsets opened but no samples published.
//
// Callers gate this open on the per-segment budget (see allocate_sort_key_sample_budget), so the
// counter must be incremented only where that gate passed AND the open succeeded.
void note_sort_key_sampling_rowset_opened();

// Read accessor for the counter above; a process-global monotonic counter, so read it as a
// before/after DELTA.
int64_t sort_key_sampling_rowsets_opened_count();

} // namespace starrocks
