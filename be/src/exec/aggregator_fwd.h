#pragma once

#include <cstddef>
#include <memory>

#include "common/system/cpu_info.h"

namespace starrocks {
namespace agg {
// Threshold at which a single-level hash set/map is converted to two-level.
// Sized to the detected L3 cache (falling back to L2, then a default) so the
// conversion tracks the last-level cache instead of a fixed assumption. Debug
// builds use a tiny value so tests exercise the two-level path cheaply.
inline size_t two_level_memory_threshold() {
#ifdef NDEBUG
    return CpuInfo::get_l3_cache_size();
#else
    return 64;
#endif
}
} // namespace agg

class Aggregator;
class SortedStreamingAggregator;
using AggregatorPtr = std::shared_ptr<Aggregator>;
using SortedStreamingAggregatorPtr = std::shared_ptr<SortedStreamingAggregator>;

template <class HashMapWithKey>
struct AllocateState;

template <class T>
class AggregatorFactoryBase;

using AggregatorFactory = AggregatorFactoryBase<Aggregator>;
using AggregatorFactoryPtr = std::shared_ptr<AggregatorFactory>;

using StreamingAggregatorFactory = AggregatorFactoryBase<SortedStreamingAggregator>;
using StreamingAggregatorFactoryPtr = std::shared_ptr<StreamingAggregatorFactory>;

} // namespace starrocks