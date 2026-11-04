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

#include "storage/sort_key_sampler.h"

#include <butil/time.h>
#include <bvar/bvar.h>

#include <algorithm>
#include <numeric>
#include <utility>

#include "base/failpoint/fail_point.h"
#include "base/utility/defer_op.h"
#include "column/chunk.h"
#include "column/chunk_factory.h"
#include "column/field.h"
#include "column/schema.h"
#include "common/config_rowset_fwd.h"
#include "storage/base/short_key_index.h"
#include "storage/chunk_helper.h"
#include "storage/chunk_variant_helper.h"
#include "storage/olap_common.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/segment.h"
#include "storage/tablet_schema.h"
#include "storage_primitive/key_coder.h"
#include "storage_primitive/range.h"
#include "types/storage_type_traits.h"

namespace starrocks {

size_t sort_key_fixed_encode_size(LogicalType lt) {
    // These arms must stay in lockstep with decode_fixed_key_column below: whatever this reports as
    // decodable, that decoder must be able to decode.
    switch (delegate_type(lt)) {
#define M(TYPE) \
    case TYPE:  \
        return StorageCppTypeSize<TYPE>;
        M(TYPE_BOOLEAN)
        M(TYPE_TINYINT)
        M(TYPE_SMALLINT)
        M(TYPE_INT)
        M(TYPE_UNSIGNED_INT)
        M(TYPE_BIGINT)
        M(TYPE_UNSIGNED_BIGINT)
        M(TYPE_LARGEINT)
        M(TYPE_DATE_V1)
        M(TYPE_DATE)
        M(TYPE_DATETIME_V1)
        M(TYPE_DATETIME)
        M(TYPE_DECIMAL)
        M(TYPE_DECIMALV2)
#undef M
    default:
        return 0;
    }
}

namespace {

// Decode one fixed-size key column. Switching on the LOGICAL type (never delegate_type) is
// load-bearing: get_key_coder resolves the delegate internally, but StorageCppType<LT> and the
// TypeInfoPtr we emplace must stay the logical type, because DatumVariant::compare DCHECKs
// _type->type() equality. Typing a DECIMAL32 sample as INT would
// DCHECK in debug and compare as the wrong type in release -- a silently wrong split boundary.
// Covers exactly the types sort_key_fixed_encode_size above admits.
template <LogicalType LT>
Status decode_fixed_key_column(Slice* s, MemPool* pool, Datum* out) {
    using CppType = StorageCppType<LT>;
    CppType raw{};
    RETURN_IF_ERROR(
            get_key_coder(LT)->decode_ascending(s, StorageCppTypeSize<LT>, reinterpret_cast<uint8_t*>(&raw), pool));
    out->set<CppType>(raw);
    return Status::OK();
}

Status decode_fixed_key_column_by_type(LogicalType lt, Slice* s, MemPool* pool, Datum* out) {
    switch (lt) {
#define M(TYPE) \
    case TYPE:  \
        return decode_fixed_key_column<TYPE>(s, pool, out);
        M(TYPE_BOOLEAN)
        M(TYPE_TINYINT)
        M(TYPE_SMALLINT)
        M(TYPE_INT)
        M(TYPE_UNSIGNED_INT)
        M(TYPE_BIGINT)
        M(TYPE_UNSIGNED_BIGINT)
        M(TYPE_LARGEINT)
        M(TYPE_DATE_V1)
        M(TYPE_DATE)
        M(TYPE_DATETIME_V1)
        M(TYPE_DATETIME)
        M(TYPE_DECIMAL)
        M(TYPE_DECIMALV2)
        M(TYPE_DECIMAL32)
        M(TYPE_DECIMAL64)
        M(TYPE_DECIMAL128)
#undef M
    default:
        // TYPE_INT256 / TYPE_DECIMAL256 are deliberately absent: their short-key encoder writes
        // nothing, so sort_key_fixed_encode_size rejects them and this is unreachable except
        // through a predicate bug -- in which case an error is exactly right, since it routes the
        // caller to path B instead of decoding bytes that were never written.
        return Status::NotSupported("short key entry: unsupported key column type " + type_to_string(lt));
    }
}

} // namespace

Status decode_short_key_entry(const Slice& encoded, const Schema& schema, const std::vector<uint32_t>& sort_key_idxes,
                              VariantTuple* out) {
    out->clear();
    out->reserve(sort_key_idxes.size());
    Slice s = encoded;
    // Nothing fixed allocates from this; it exists only to satisfy decode_ascending's signature.
    MemPool pool;
    for (uint32_t cid : sort_key_idxes) {
        if (s.size == 0 || cid >= schema.num_fields()) {
            return Status::InvalidArgument("short key entry: malformed encoded buffer or column index");
        }
        const auto marker = static_cast<uint8_t>(s.data[0]);
        s.remove_prefix(1);
        const FieldPtr& field = schema.field(cid);
        if (marker == KEY_NULL_FIRST_MARKER) {
            Datum null_datum;
            null_datum.set_null();
            out->emplace(field->type(), null_datum);
            continue;
        }
        if (marker != KEY_NORMAL_MARKER) {
            return Status::InvalidArgument("short key entry: bad marker byte in encoded buffer");
        }
        Datum value;
        RETURN_IF_ERROR(decode_fixed_key_column_by_type(field->type()->type(), &s, &pool, &value));
        out->emplace(field->type(), value);
    }
    return Status::OK();
}

bool short_key_index_encodes_full_sort_key(const Schema& schema, const std::vector<uint32_t>& sort_key_idxes,
                                           size_t num_short_key_columns) {
    if (sort_key_idxes.empty() || num_short_key_columns != sort_key_idxes.size()) {
        return false;
    }
    for (uint32_t cid : sort_key_idxes) {
        if (cid >= schema.num_fields()) {
            return false;
        }
        if (sort_key_fixed_encode_size(schema.field(cid)->type()->type()) == 0) {
            return false;
        }
    }
    // Deliberately no short_key_length() check: every fixed encoder ignores index_size and writes
    // kFullEncodeSize bytes unconditionally, and a fixed-size key column cannot have a zero index
    // length in production. Correctness rests on the entry-0 == sort_key_min comparison in
    // sample_sort_key_from_short_key_index, which catches any encoding mismatch empirically.
    return true;
}

namespace {
// Path-agnostic: counts published samples regardless of which path produced them.
bvar::Adder<int64_t> g_sort_key_sampling_samples_total("sort_key_sampling_samples_total");
// Latency and fallback-rejection counters are per-path, NOT shared: a caller that tries path A, gets
// empty, and escalates to path B would otherwise charge one shared latency recorder twice for a
// single segment, mixing an index-page-only latency distribution with a data-page-read one into one
// percentile series; and a single fallback counter could not tell a path-A schema/geometry rejection
// from a path-B I/O failure -- operationally very different signals.
bvar::LatencyRecorder g_sort_key_sampling_short_key_index_latency("sort_key_sampling", "short_key_index");
bvar::Adder<int64_t> g_sort_key_sampling_short_key_index_fallback_total("sort_key_sampling",
                                                                        "short_key_index_fallback_total");
bvar::LatencyRecorder g_sort_key_sampling_data_page_latency("sort_key_sampling", "data_page");
bvar::Adder<int64_t> g_sort_key_sampling_data_page_fallback_total("sort_key_sampling", "data_page_fallback_total");
bvar::Adder<int64_t> g_sort_key_sampling_data_page_segments_total("sort_key_sampling_data_page_segments_total");
bvar::Adder<int64_t> g_sort_key_sampling_read_bytes_total("sort_key_sampling_read_bytes_total");
// Incremented by the CALLERS, not by this module: opening a rowset's segment files is their work,
// and it happens before any function here is entered. The two counters above describe sampling that
// already got as far as a segment; this one is the only signal for the I/O spent getting there,
// which is also the only way a budget that never reaches the segments being compacted is visible.
bvar::Adder<int64_t> g_sort_key_sampling_rowsets_opened_total("sort_key_sampling_rowsets_opened_total");

// The carrier invariant the consumer depends on, written overflow-safely: the consumer assigns
// row_interval rows to each of |num_samples| sub-segments and the remainder to a tail it requires
// to be non-empty, i.e. num_samples * row_interval < num_rows. Both sampling paths below call this
// at their single publish point instead of duplicating the arithmetic.
bool samples_fit_row_count(int64_t num_samples, int64_t row_interval, int64_t num_rows) {
    return row_interval > 0 && num_rows > 0 && num_samples <= (num_rows - 1) / row_interval;
}
} // namespace

int64_t sort_key_sampling_data_page_segments_count() {
    return g_sort_key_sampling_data_page_segments_total.get_value();
}

int64_t sort_key_sampling_samples_count() {
    return g_sort_key_sampling_samples_total.get_value();
}

int64_t sort_key_sampling_data_page_fallback_count() {
    return g_sort_key_sampling_data_page_fallback_total.get_value();
}

int64_t sort_key_sampling_short_key_index_latency_count() {
    return g_sort_key_sampling_short_key_index_latency.count();
}

void note_sort_key_sampling_rowset_opened() {
    g_sort_key_sampling_rowsets_opened_total << 1;
}

int64_t sort_key_sampling_rowsets_opened_count() {
    return g_sort_key_sampling_rowsets_opened_total.get_value();
}

// Test-only: the third clause of the geometry triple below (num_items == ceil(num_rows /
// block_rows)) cannot be violated without doctoring footer bytes, since block_rows, num_items and
// num_rows all come from the same segment. This failpoint halves the block_rows value the function
// below just read, letting a unit test exercise that clause directly. See
// sort_key_sampler_test.cpp's path_a_rejects_a_block_size_that_contradicts_the_entry_count.
DEFINE_SCOPED_FAIL_POINT(sort_key_sampler_perturb_block_rows);

StatusOr<SortKeySamples> sample_sort_key_from_short_key_index(Segment& segment, const Schema& schema,
                                                              const std::vector<uint32_t>& sort_key_idxes,
                                                              const VariantTuple& min_key, const VariantTuple& max_key,
                                                              int64_t num_rows, int64_t target) {
    SortKeySamples out;
    // Nothing to do -- no budget was apportioned to this segment. Checked before the latency timer
    // starts, so a covered segment with a zero budget does not charge a zero-duration sample into
    // the histogram below and dilute it with segments that did no work at all.
    if (target <= 0) {
        return out;
    }
    // Charges latency on every exit from here on, including every fail-open path below -- a slow
    // tablet that keeps failing open must still show up here. No bytes to charge: this path reads
    // only the already-parsed short key index page, never a data page.
    const int64_t start_us = butil::gettimeofday_us();
    DeferOp account([&] { g_sort_key_sampling_short_key_index_latency << (butil::gettimeofday_us() - start_us); });
    const ShortKeyIndexDecoder* decoder = segment.decoder();
    if (decoder == nullptr) {
        g_sort_key_sampling_short_key_index_fallback_total << 1;
        return out;
    }
    // EVERY number below comes from the page footer, which ShortKeyIndexDecoder::parse accepts
    // without cross-checking against anything. |num_rows| comes from SegmentMetadataPB. Requiring
    // the two to agree is what rejects a footer whose block size is INCONSISTENT with its own item
    // count.
    //
    // It does NOT pin the stride, and the limit is worth stating exactly rather than overclaiming.
    // `num_items == ceil(num_rows / block_rows)` is many-to-one: for n entries the equation admits
    // every block size in [num_rows/n, num_rows/(n-1)), so 8192 rows with 8 entries accepts both
    // 1024 (what a writer would emit) and, say, 1170. A footer claiming 1170 while its entries were
    // written at 1024 passes this check, the bounds checks, monotonicity and the carrier check, and
    // then attributes each sample to the wrong ordinal.
    //
    // Nothing in the index page can distinguish the two, so closing this would cost a data-page read
    // and forfeit the whole point of this path. It is accepted deliberately, on this reasoning:
    // SegmentWriter builds ShortKeyIndexBuilder from the same `_opts.num_rows_per_block` it strides
    // on, so the two cannot disagree in a segment any writer produced -- only in corrupted or forged
    // metadata. The worst outcome there is an UNEVEN split, not a wrong one: the boundaries are still
    // real keys from the index, still monotone and still inside [sort_key_min, sort_key_max], and the
    // carrier invariant still holds, so no consumer sees a negative row delta. The code this replaced
    // trusted a persisted row interval outright, so this is strictly narrower trust than before.
    int64_t block_rows = decoder->num_rows_per_block();
    FAIL_POINT_TRIGGER_EXECUTE(sort_key_sampler_perturb_block_rows, { block_rows /= 2; });
    const int64_t num_items = decoder->num_items();
    // THREE independent sources must agree, not two: SegmentMetadataPB (num_rows), the short key
    // footer (num_segment_rows/num_rows_per_block/num_items), and the segment footer
    // (segment.num_rows()). Metadata and the index footer can agree with each other while both
    // disagree with the real segment -- e.g. a footer rewritten to 4096 rows at a 512-row block
    // keeps 8 entries and satisfies the identity while reporting half the true stride, which
    // mis-weights every row it assigns. Path B makes the same three-way check.
    //
    // In a well-formed segment all three ARE the same value by construction -- SegmentWriter::
    // _num_rows, settled in finalize_columns before the index page is written, and
    // Segment::set_num_rows has no production callers -- so this triple costs nothing on the happy
    // path. It also earns its keep twice over:
    //   * it is the only cross-source verification in this design; dropping the
    //     num_rows == segment.num_rows() clause re-opens the release-silent mis-weighting that the
    //     consumer cannot detect (its own DCHECKs compile out);
    //   * Segment::num_rows() is uint32, so agreeing with it is what makes stride * block_rows
    //     provably overflow-free below without reaching for __int128.
    //
    // DO NOT remove the num_rows <= 0 guard as "impossible". A replicated rowset
    // (shared-nothing -> shared-data) has SegmentMetadataPB carrying ONLY filename and
    // encryption_meta -- ReplicationTxnManager::convert_rowset_meta never sets num_rows, size or the
    // sort key bounds -- so num_rows is legitimately 0 while both footers hold the true count. That
    // guard is what makes such a segment fall through cleanly instead of failing the triple; the
    // consumer skips it anyway (distribute_segment_to_ranges's num_rows == 0 && data_size == 0 early
    // return), and it has no bounds to contribute, so this is pre-existing behavior rather than a
    // regression.
    if (block_rows <= 0 || num_rows <= 0 || num_rows != static_cast<int64_t>(segment.num_rows()) ||
        decoder->num_segment_rows() != num_rows || num_items != (num_rows + block_rows - 1) / block_rows) {
        g_sort_key_sampling_short_key_index_fallback_total << 1;
        return out;
    }
    if (num_items < 2) {
        // Entry 0 is row 0, not a sample. Returning empty sends the caller to path B, which can
        // still sample a sub-block segment at row granularity.
        return out;
    }

    // Entry 0 is always row 0 (SegmentWriter emits an entry when num_rows_written % rows_per_block
    // == 0) and sort_key_min is row 0 of the first chunk, so under the coverage predicate the two
    // must decode equal. This single comparison empirically subsumes every static assumption the
    // predicate makes -- a mis-resolved historical schema, an arity mismatch, a short key that
    // predates being encoded from the sort key columns -- so do NOT add static guards for those.
    VariantTuple first;
    if (!decode_short_key_entry(decoder->key(0), schema, sort_key_idxes, &first).ok() ||
        (!min_key.empty() && first.compare(min_key) != 0)) {
        g_sort_key_sampling_short_key_index_fallback_total << 1;
        return out;
    }

    const int64_t candidates = num_items - 1;
    const int64_t stride = std::max<int64_t>(1, (candidates + target - 1) / target);
    std::vector<VariantTuple> decoded;
    decoded.reserve(candidates / stride);
    for (int64_t i = stride; i < num_items; i += stride) {
        VariantTuple sample;
        if (!decode_short_key_entry(decoder->key(i), schema, sort_key_idxes, &sample).ok() ||
            sample.size() != sort_key_idxes.size() || (!decoded.empty() && decoded.back().compare(sample) > 0) ||
            (!min_key.empty() && min_key.compare(sample) > 0) || (!max_key.empty() && sample.compare(max_key) > 0)) {
            g_sort_key_sampling_short_key_index_fallback_total << 1;
            return out;
        }
        decoded.push_back(std::move(sample));
    }
    if (!decoded.empty()) {
        const int64_t row_interval = stride * block_rows;
        // The GEOMETRY TRIPLE above is what turns the stride algebra from an assumption into a
        // fact -- it is the design's only cross-source verification. This is the belt-and-braces
        // re-check described on samples_fit_row_count; it cannot fire while that triple holds.
        if (!samples_fit_row_count(static_cast<int64_t>(decoded.size()), row_interval, num_rows)) {
            g_sort_key_sampling_short_key_index_fallback_total << 1;
            return out;
        }
        out.row_interval = row_interval;
        out.samples = std::move(decoded);
        g_sort_key_sampling_samples_total << static_cast<int64_t>(out.samples.size());
    }
    return out;
}

// Test-only: makes the NEXT per-column length check observe a column that read fewer rows than the
// range asked for, without needing a segment that genuinely short-reads (a well-formed segment with
// num_rows == segment.num_rows() cannot produce one). See
// sort_key_sampler_test.cpp's path_b_refuses_a_short_read_column.
DEFINE_SCOPED_FAIL_POINT(sort_key_sampler_short_read_one_column);

StatusOr<SortKeySamples> sample_sort_key_from_segment_data(Segment& segment, const TabletSchemaCSPtr& segment_schema,
                                                           const std::vector<uint32_t>& sort_key_idxes,
                                                           int64_t num_rows, const VariantTuple& min_key,
                                                           const VariantTuple& max_key, int64_t target,
                                                           bool fill_data_cache) {
    // Nothing to do -- no budget was apportioned to this segment, too few rows to sample, or no
    // sort key to sample. Checked before the latency timer starts, so this case does not charge a
    // zero-duration sample into the histogram below and dilute it with segments that did no work.
    if (target <= 0 || num_rows <= 1 || sort_key_idxes.empty()) {
        return SortKeySamples{};
    }
    // Charges latency on every path from here on, and bytes on every path that got far enough to
    // read any. These must be the first two statements after the guard above: every remaining guard
    // below, including the fail-open ones, must be covered, or a slow tablet that keeps failing
    // open records nothing.
    OlapReaderStatistics stats;
    const int64_t start_us = butil::gettimeofday_us();
    DeferOp account([&] {
        g_sort_key_sampling_data_page_latency << (butil::gettimeofday_us() - start_us);
        // compressed_bytes_read is written only by SegmentIterator::_update_stats, which this path
        // never runs; the field the page reads actually fill is compressed_bytes_read_request, set
        // by read_page_from_file (rowset/page_io.cpp).
        g_sort_key_sampling_read_bytes_total << static_cast<int64_t>(stats.compressed_bytes_read_request);
    });
    // num_rows is bounded to uint32 by the segment.num_rows() check below, but target is an
    // unbounded int64_t parameter; without clamping it, a target near INT64_MAX makes
    // (num_rows + target) below signed-overflow UB. Semantics-preserving: any target >= num_rows
    // already yields row_interval == 1.
    target = std::min<int64_t>(target, num_rows);
    // num_rows is SegmentMetadataPB's, independent of the segment's own footer count, and ordinals
    // are cast to rowid_t and fed to the reader. Requiring equality with segment.num_rows() covers
    // the rowid_t bound for free, because that accessor is already uint32-wide -- so there is
    // deliberately no separate numeric_limits<rowid_t> check, which would be unreachable.
    if (num_rows != static_cast<int64_t>(segment.num_rows())) {
        g_sort_key_sampling_data_page_fallback_total << 1;
        return SortKeySamples{};
    }
    // ChunkHelper::get_sort_key_schema types and orders the destination chunk from
    // segment_schema->sort_key_idxes(), but the fill loop below iterates the sort_key_idxes
    // PARAMETER by position. Nothing else checks the two agree: a permuted same-type parameter
    // would publish silently permuted split boundaries that still pass monotonicity and bounds, and
    // a longer parameter is release-mode out-of-bounds (Chunk::get_column_raw_ptr_by_index,
    // Schema::field and TabletSchema::column all bounds-check with a DCHECK only). Path A already
    // rejects an out-of-range column id (see short_key_index_encodes_full_sort_key above); this is
    // path B's equivalent.
    if (sort_key_idxes != segment_schema->sort_key_idxes()) {
        g_sort_key_sampling_data_page_fallback_total << 1;
        return SortKeySamples{};
    }
    // The max<int64_t>(1, ...) cannot fire: num_rows + target >= target + 1 whenever num_rows >= 1,
    // so the quotient is already at least 1. It is retained to state the postcondition the loop
    // below depends on -- row_interval >= 1, or the loop would not terminate -- and NOT as any kind
    // of protection for the division, which it cannot provide: the division is evaluated as this
    // call's argument, before max() runs.
    //
    // What makes the division itself safe is the `target <= 0` rejection at the top of this
    // function, which alone keeps target + 1 away from zero; the clamp just above only bounds the
    // upper end, so that num_rows + target cannot overflow. Both must stay, for those two separate
    // reasons.
    const int64_t row_interval = std::max<int64_t>(1, (num_rows + target) / (target + 1));
    SparseRange<> range;
    int64_t num_ordinals = 0;
    for (int64_t ord = row_interval; ord < num_rows; ord += row_interval) {
        range.add(Range<>(static_cast<rowid_t>(ord), static_cast<rowid_t>(ord + 1)));
        ++num_ordinals;
    }
    // Cannot fire, and unreachable for exactly one reason: the `target <= 0` rejection at the top
    // of this function is what guarantees target >= 1 here, and given that plus num_rows >= 2
    // (also enforced above), row_interval is always strictly less than num_rows --
    // floor((num_rows+target)/(target+1)) >= num_rows would require target*(1-num_rows) >= 0, which
    // target >= 1 and num_rows >= 2 make false -- so the loop above always adds at least one
    // ordinal. That SAME `target <= 0` rejection is also what guards the target + 1 divide-by-zero
    // above: do not delete it while believing only one of these two guards depends on it. Retained
    // anyway in this file's belt-and-braces style (mirrors path A's num_items < 2 check above).
    if (num_ordinals == 0) {
        return SortKeySamples{};
    }

    // Counted here, before the first read, because this is the point where the segment is committed
    // to the data-page path: everything above is arithmetic that can still decline it, everything
    // below either reads pages or fails open having tried. Moving it past the reads would make a
    // segment whose read fails look like one path A handled for free, which is the opposite of what
    // this counter exists to show; the bytes counter above is what answers "how much I/O did we
    // actually pay". See its contract in sort_key_sampler.h.
    g_sort_key_sampling_data_page_segments_total << 1;

    // NOTE the non-const Segment&: Segment::new_segment_read_file, Segment::new_column_iterator and
    // Segment::load_index are all non-const, so a const reference does not compile. The opened
    // segments are already held as mutable Segment* by both consumers.
    //
    // Sampling is fail-open, so NOTHING here may use ASSIGN_OR_RETURN / RETURN_IF_ERROR: propagating
    // a transient read error would fail the whole split instead of degrading to coarse boundaries.
    //
    // Segment::new_segment_read_file already does the filesystem, bundling, data-cache and KeyCache
    // encryption setup, so do not hand-roll that sequence. The handle must outlive every iterator,
    // because ColumnIteratorOptions::read_file is a raw non-owning pointer.
    const LakeIOOptions lake_io_opts{.fill_data_cache = fill_data_cache};
    auto read_file_or = segment.new_segment_read_file(lake_io_opts);
    if (!read_file_or.ok()) {
        g_sort_key_sampling_data_page_fallback_total << 1;
        return SortKeySamples{};
    }
    auto read_file = std::move(read_file_or).value();

    // Build the destination from the Schema so nullability is correct by construction: a nullable
    // reader writing into a non-nullable column is undefined behaviour, and only on the pages that
    // actually carry a null.
    Schema sort_key_schema = ChunkHelper::get_sort_key_schema(segment_schema);
    auto chunk = ChunkFactory::new_chunk(sort_key_schema, static_cast<size_t>(num_ordinals));
    for (size_t i = 0; i < sort_key_idxes.size(); ++i) {
        const auto& tablet_column = segment_schema->column(sort_key_idxes[i]);
        auto iterator_or = segment.new_column_iterator(tablet_column, /*path=*/nullptr);
        if (!iterator_or.ok()) {
            // NotFound: this segment predates a trailing sort-key column add. new_column_iterator_
            // or_default would hand back defaults, making every sample identical in that position,
            // so refuse to sample instead.
            g_sort_key_sampling_data_page_fallback_total << 1;
            return SortKeySamples{};
        }
        auto iterator = std::move(iterator_or).value();

        ColumnIteratorOptions iter_opts;
        iter_opts.read_file = read_file.get();
        iter_opts.stats = &stats;
        iter_opts.lake_io_opts = lake_io_opts;
        // A background split-point computation should not evict query pages.
        iter_opts.temporary_data = true;
        if (auto st = iterator->init(iter_opts); !st.ok()) {
            g_sort_key_sampling_data_page_fallback_total << 1;
            return SortKeySamples{};
        }

        auto* column = chunk->get_column_raw_ptr_by_index(i);
        // ScalarColumnIterator overrides next_batch(SparseRange) and the override does NOT seek;
        // its first act is to dereference the current page. Position once, here.
        if (auto st = iterator->seek_to_ordinal(range.begin()); !st.ok()) {
            g_sort_key_sampling_data_page_fallback_total << 1;
            return SortKeySamples{};
        }
        if (auto st = iterator->next_batch(range, column); !st.ok()) {
            g_sort_key_sampling_data_page_fallback_total << 1;
            return SortKeySamples{};
        }
        FAIL_POINT_TRIGGER_EXECUTE(sort_key_sampler_short_read_one_column, {
            if (i == 0 && column->size() > 0) column->resize(column->size() - 1);
        });
        // next_batch can legitimately return OK having read fewer rows than the range asked for
        // (e.g. it reached EOF). Setting the chunk's row count above what a column actually holds
        // would then make the tuple loop below read out of bounds, so check every column.
        if (column->size() != static_cast<size_t>(num_ordinals)) {
            g_sort_key_sampling_data_page_fallback_total << 1;
            return SortKeySamples{};
        }
        if (sort_key_schema.field(i)->type()->type() == TYPE_CHAR) {
            ChunkHelper::padding_char_column(segment_schema, *sort_key_schema.field(i), column);
        }
    }
    chunk->set_num_rows(static_cast<size_t>(num_ordinals));

    std::vector<uint32_t> positions(sort_key_idxes.size());
    std::iota(positions.begin(), positions.end(), 0u);
    SortKeySamples out;
    out.samples.reserve(static_cast<size_t>(num_ordinals));
    for (int64_t k = 0; k < num_ordinals; ++k) {
        VariantTuple sample = build_variant_tuple_from_chunk_row(*chunk, static_cast<size_t>(k), positions);
        if ((!out.samples.empty() && out.samples.back().compare(sample) > 0) ||
            (!min_key.empty() && min_key.compare(sample) > 0) || (!max_key.empty() && sample.compare(max_key) > 0)) {
            g_sort_key_sampling_data_page_fallback_total << 1;
            return SortKeySamples{};
        }
        out.samples.push_back(std::move(sample));
    }
    // num_ordinals IS floor((num_rows-1)/row_interval) by the loop bound above, so this predicate
    // is the identity x <= x and cannot fire -- both operands are locals derived from each other.
    // Retained anyway for the reason on samples_fit_row_count: it is the same one-division assertion
    // of the consumer contract, applied at this path's single publish point.
    if (!samples_fit_row_count(static_cast<int64_t>(out.samples.size()), row_interval, num_rows)) {
        g_sort_key_sampling_data_page_fallback_total << 1;
        return SortKeySamples{};
    }
    out.row_interval = row_interval;
    g_sort_key_sampling_samples_total << static_cast<int64_t>(out.samples.size());
    return out;
}

std::vector<int64_t> allocate_sort_key_sample_budget(const std::vector<int64_t>& segment_num_rows,
                                                     int64_t split_width) {
    std::vector<int64_t> budget(segment_num_rows.size(), 0);
    const int64_t cap = config::sort_key_max_samples_per_tablet;
    if (cap <= 0 || split_width < 2 || segment_num_rows.empty()) {
        return budget;
    }
    // segment_num_rows comes from SegmentMetadataPB, i.e. from object storage, so it is int64 that
    // this function does not get to trust. Accumulate and multiply in __int128 so malformed
    // metadata cannot make the arithmetic undefined. The codebase already reaches for __int128 in
    // exactly this situation (the bytes_for lambda in distribute_segment_to_ranges,
    // storage/lake/tablet_splitter.cpp).
    __int128 total_rows = 0;
    for (int64_t rows : segment_num_rows) {
        if (rows > 0) {
            total_rows += rows;
        }
    }
    // Cannot fire, and unreachable for exactly one reason: the inner `segment_num_rows[i] <= 0` skip
    // in the loop below is what guarantees every row counted into total_rows came from an element
    // that also passes that skip, so total_rows <= 0 here iff every element is skipped there too --
    // meaning the share division below is never reached either way. This is the same paired-guard
    // shape as `target <= 0` / `num_ordinals == 0` in sample_sort_key_from_segment_data above:
    // remove the inner skip and THIS guard becomes the only thing standing between the share
    // computation and a division by zero. Do not delete either on the strength of the other looking
    // redundant. Retained anyway in this file's belt-and-braces style.
    if (total_rows <= 0) {
        return budget;
    }
    // Saturate split_width before multiplying: cap bounds the product's usefulness anyway, so
    // clamping the input is enough and keeps the multiply in range. This clamp is overflow-only --
    // for any split_width > cap / kSortKeySamplesPerSplit the defined result (target_total == cap)
    // is identical with or without it, so no test can observe a difference on a defined input.
    // Without it, kSortKeySamplesPerSplit * split_width overflows int64_t (UB) once split_width
    // exceeds roughly INT64_MAX / kSortKeySamplesPerSplit ~= 2.9e17; a UBSan build, not a unit test,
    // is what would catch this clamp's removal.
    const int64_t bounded_width = std::min<int64_t>(split_width, cap);
    const int64_t target_total = std::min<int64_t>(cap, kSortKeySamplesPerSplit * bounded_width);
    // Apportion by largest remainder, not by an independently floored share. Flooring alone loses
    // the whole budget once a tablet has more segments than samples: at the default cap a 2-way
    // split targets 64 samples, so 65 equally sized segments each floor to 64/65 == 0, every
    // rowset then fails rowset_wants_samples, and the tablet is sampled not at all -- turning the
    // feature off precisely on the fragmented tablets that need it most. Worse than coarse where
    // the segments' coarse bounds coincide (repeated loads over one key domain): the endpoints
    // deduplicate to a single candidate range and the split is REFUSED with "Not enough split
    // ranges available".
    //
    // Floors first, then hand the residual to the largest remainders. No per-segment ceiling is
    // needed -- target_total bounds the tablet -- and the residual is smaller than the number of
    // positive segments by construction (each floor discards strictly less than one whole share),
    // so a segment is incremented at most once and the shares sum to exactly target_total.
    std::vector<std::pair<__int128, size_t>> remainders; // (remainder, index), positive segments only
    remainders.reserve(segment_num_rows.size());
    __int128 allocated = 0;
    for (size_t i = 0; i < segment_num_rows.size(); ++i) {
        if (segment_num_rows[i] <= 0) {
            continue;
        }
        const __int128 scaled = static_cast<__int128>(target_total) * segment_num_rows[i];
        const __int128 share = scaled / total_rows;
        budget[i] = static_cast<int64_t>(share);
        allocated += share;
        remainders.emplace_back(scaled % total_rows, i);
    }
    // Clamped for the same belt-and-braces reason as the guards above: the construction argument
    // says residual < remainders.size(), and nothing downstream would survive an out-of-range
    // nth_element if that argument were ever broken by an edit.
    const size_t residual =
            std::min<size_t>(static_cast<size_t>(std::max<__int128>(0, target_total - allocated)), remainders.size());
    if (residual > 0) {
        // Largest remainder first, ties by lowest index. Indices are unique, so this is a strict
        // total order and the chosen set is deterministic -- nth_element's unspecified ordering
        // among equivalent elements cannot leak into the result, which matters because equal-sized
        // segments produce exactly equal remainders.
        const auto by_remainder_desc = [](const std::pair<__int128, size_t>& a, const std::pair<__int128, size_t>& b) {
            if (a.first != b.first) return a.first > b.first;
            return a.second < b.second;
        };
        std::nth_element(remainders.begin(), remainders.begin() + residual, remainders.end(), by_remainder_desc);
        for (size_t k = 0; k < residual; ++k) {
            budget[remainders[k].second] += 1;
        }
    }
    return budget;
}

} // namespace starrocks
