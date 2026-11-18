# Shared Lake metadata memory-owner regression contract

## Fixed scope

Lake `TabletManager::load_segment` uses `Segment::create` to allocate the Segment,
its owned FileInfo strings, retained footer/ColumnReader metadata and lazy
ordinal/zone-map/bitmap/bloom/short-key indexes under the process tracker. The
final deleter scopes the entire `delete`, not merely the destructor body.
`ProcessMemoryAllocator` separately covers shared_ptr control-block storage,
including weak-only lifetime after the payload has been destroyed.

The final Segment deleter detaches externally supplied FileSystem/TabletSchema
references and recursively releases inverted-index readers **before** entering
the process scope. Those objects keep the existing caller attribution. A final
Segment has no live iterator owner, so its unique-owned ColumnReaders are not
being mutated while these references are detached. Null readers are allowed.
File creation, KeyCache unwrap, borrowed-stream IO and corruption-cache
invalidation also stay in the caller context; owned encryption information is
deep-copied, not moved across the owner boundary. Temporary IndexReadOptions
copies and ZoneMap's temporary iterator cannot retain the borrowed tracker past
the synchronous load.

Metacache injects process-owned storage callbacks for LRU handles/hash buckets and CacheValue
storage, but its client payload deleters run in the original caller context.
Other LRU users keep the default policy. Metadata-cache size updates that can
evict unrelated values run outside the Segment metadata scope.

This is **not** a generic allocation-owner implementation. It does not change
MemTracker destruction, clamp negative counters or clear global counters.
It does not fix existing cross-task ownership of arbitrary Metacache payloads
(TabletMetadataPB, TxnLogPB, schema, delvec), Rowsets, StarOS/SDK/KeyCache data,
CLucene objects, iterator scratch or directly constructed legacy Segments.

## Limits and cost

Owned shared metadata now contributes to **process**, not the Compaction
subtree. Compaction's hard limit continues to cover task scratch/iterators and
other allocations still charged there; it is no longer a cap on this shared
metadata. A held-Segment budget is only a sampled chunk/fallback decision, not
an allocation-owner hard cap. Root is still an ancestor of task trackers, so
cooperative `check_mem_limit` observes process excess. Where caught-allocation
mode is enabled, the existing production hook rejects process-limit excess.
This helper does not enable caught-allocation mode for paths that did not
already use it, and does not remove existing deferred-check behavior.

A real TLS tracker switch commits the batched counter and returns unused
reservation. A success owner scope has two switches, with two additional
switches per borrowed IO operation; errors copy Status into the returning
context and have extra switches. Same-tracker scopes are no-ops. The design adds
one allocation versus make_shared, a copied FileInfo, and opt-in LRU bookkeeping
scope costs. It adds no per-allocation production map/header. Peak/throughput
regressions must be measured in the real Linux workload, not inferred from the
small scope tests.

## Tests and evidence boundaries

- `ProcessMemoryTest.*`: scope restoration on success/error/exception and
  storage-only allocator versus payload destructor context.
- `SegmentReaderWriterTest.ProcessOwned*`: actual factory dependency release,
  real footer/short-key reading, and actual footer/PageIO failure/exception
  routing through a borrowed stream.
- `CacheStorageAllocatorTest.CoversRejectedHandlesResizesAndEvictionWithoutScopingPayloads`: opt-in LRU
  eviction does not change arbitrary client deleter context.

Normal BE_TEST replaces the production malloc hook: the above UTs test scope
contracts, not production byte conservation. A non-BE_TEST regression must also
observe actual allocations, including C++ new/delete, members and weak control
blocks. The strict negative control must reach this sequence without an early
"A must be zero" assertion:

1. A allocates the shared object; B finally frees it.
2. Verify physical allocated bytes and process return to baseline.
3. Destroy B's tracker while A remains alive.
4. Assert Compaction remains at baseline (the original implementation fails).

Repeat both lifetime orders and cross-category release, real LRU eviction with
an external reference, lazy loads, error/exception paths, multi-round churn,
batching thresholds and reservations. Also verify genuine process and task
scratch limits still reject. A source slice with modeled dependencies is useful
for this mechanism but is not full Linux BE/StarOS integration evidence.

Before release, run full target build/UT and production hooks with real
Segment/ColumnReader/protobuf, encrypted IO/cache retries, concurrent lazy loads,
wide/nested columns, and process peaks with the held-Segment backport. Existing
unfixed shared dependencies must not be described as fully owner-accounted.
