#pragma once

#include <cstddef>
#include <memory>

namespace starrocks {
namespace agg {
#ifdef NDEBUG
constexpr size_t two_level_memory_threshold = 33554432; // 32M, L3 Cache
#else
constexpr size_t two_level_memory_threshold = 64;
#endif
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