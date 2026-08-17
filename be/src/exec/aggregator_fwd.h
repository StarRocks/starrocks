#pragma once

#include <memory>

namespace starrocks {

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
