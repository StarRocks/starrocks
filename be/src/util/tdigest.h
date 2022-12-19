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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/util/tdigest.h

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

/*
 * Licensed to Derrick R. Burns under one or more
 * contributor license agreements.  See the NOTICES file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// T-Digest :  Percentile and Quantile Estimation of Big Data
// A new data structure for accurate on-line accumulation of rank-based statistics
// such as quantiles and trimmed means.
// See original paper: "Computing extremely accurate quantiles using t-digest"
// by Ted Dunning and Otmar Ertl for more details
// https://github.com/tdunning/t-digest/blob/07b8f2ca2be8d0a9f04df2feadad5ddc1bb73c88/docs/t-digest-paper/histo.pdf.
// https://github.com/derrickburns/tdigest

#pragma once

#include <queue>
#include <vector>

#include "exprs/function_context.h"
#include "util/slice.h"

namespace starrocks {

using Value = float;
using Weight = float;
using Index = size_t;

const size_t kHighWater = 40000;

class Centroid {
public:
    Centroid();
    Centroid(Value mean, Weight weight);

    Value mean() const noexcept;
    Weight weight() const noexcept;
    Value& mean() noexcept;
    Weight& weight() noexcept;
    void add(const Centroid& c);

private:
    Value _mean = 0;
    Weight _weight = 0;
};

struct CentroidList {
    CentroidList(const std::vector<Centroid>& s) : iter(s.cbegin()), end(s.cend()) {}
    std::vector<Centroid>::const_iterator iter;
    std::vector<Centroid>::const_iterator end;

    bool advance() { return ++iter != end; }
};

class CentroidListComparator {
public:
    CentroidListComparator() = default;

    bool operator()(const CentroidList& left, const CentroidList& right) const {
        return left.iter->mean() > right.iter->mean();
    }
};

using CentroidListQueue = std::priority_queue<CentroidList, std::vector<CentroidList>, CentroidListComparator>;

struct CentroidComparator {
    bool operator()(const Centroid& a, const Centroid& b) const { return a.mean() < b.mean(); }
};

class TDigest {
    class TDigestComparator {
    public:
        TDigestComparator() = default;

        bool operator()(const TDigest* left, const TDigest* right) const {
            return left->totalSize() > right->totalSize();
        }
    };
    using TDigestQueue = std::priority_queue<const TDigest*, std::vector<const TDigest*>, TDigestComparator>;

public:
    TDigest() : TDigest(1000) {}
    explicit TDigest(Value compression) : TDigest(compression, 0) {}
    explicit TDigest(const char* src) { this->deserialize(src); }
    explicit TDigest(const Slice& src) { this->deserialize(src.data); }
    TDigest(Value compression, Index bufferSize) : TDigest(compression, bufferSize, 0) {}
    TDigest(Value compression, Index unmergedSize, Index mergedSize);
    TDigest(std::vector<Centroid>&& processed, std::vector<Centroid>&& unprocessed, Value compression,
            Index unmergedSize, Index mergedSize);

    static Weight weight(std::vector<Centroid>& centroids) noexcept;
    static Index processedSize(Index size, Value compression) noexcept;
    static Index unprocessedSize(Index size, Value compression) noexcept;

    // merge in another t-digest
    void merge(const TDigest* other);
    const std::vector<Centroid>& processed() const;
    const std::vector<Centroid>& unprocessed() const;
    Index maxUnprocessed() const;
    Index maxProcessed() const;
    void add(const std::vector<const TDigest*>& digests);
    // merge in a vector of tdigests in the most efficient manner possible
    // in constant space
    // works for any value of kHighWater
    void add(std::vector<const TDigest*>::const_iterator iter, std::vector<const TDigest*>::const_iterator end);
    Weight processedWeight() const;
    Weight unprocessedWeight() const;
    bool haveUnprocessed() const;
    size_t totalSize() const;
    long totalWeight() const;
    // return the cdf on the t-digest
    Value cdf(Value x);
    bool isDirty();
    // return the cdf on the processed values
    Value cdfProcessed(Value x) const;
    // this returns a quantile on the t-digest
    Value quantile(Value q);
    // this returns a quantile on the currently processed values without changing the t-digest
    // the value will not represent the unprocessed values
    Value quantileProcessed(Value q) const;
    Value compression() const;
    void add(Value x);
    void compress();
    // add a single centroid to the unprocessed vector, processing previously unprocessed sorted if our limit has
    // been reached.
    bool add(Value x, Weight w);
    void add(std::vector<Centroid>::const_iterator iter, std::vector<Centroid>::const_iterator end);
    uint64_t serialize_size() const;
    size_t serialize(uint8_t* writer) const;
    void deserialize(const char* type_reader);

private:
    Value _compression;
    Value _min = std::numeric_limits<Value>::max();
    Value _max = std::numeric_limits<Value>::lowest();
    Index _max_processed;
    Index _max_unprocessed;
    Value _processed_weight = 0.0;
    Value _unprocessed_weight = 0.0;
    std::vector<Centroid> _processed;
    std::vector<Centroid> _unprocessed;
    std::vector<Weight> _cumulative;

    // return mean of i-th centroid
    Value mean(int i) const noexcept;
    // return weight of i-th centroid
    Weight weight(int i) const noexcept;
    // append all unprocessed centroids into current unprocessed vector
    void mergeUnprocessed(const std::vector<const TDigest*>& tdigests);
    // merge all processed centroids together into a single sorted vector
    void mergeProcessed(const std::vector<const TDigest*>& tdigests);
    void processIfNecessary();
    void updateCumulative();

    // merges _unprocessed centroids and _processed centroids together and processes them
    // when complete, _unprocessed will be empty and _processed will have at most _max_processed centroids
    void process();
    int checkWeights();
    size_t checkWeights(const std::vector<Centroid>& sorted, Value total);

    /**
    * Converts a quantile into a centroid scale value.  The centroid scale is nomin_ally
    * the number k of the centroid that a quantile point q should belong to.  Due to
    * round-offs, however, we can't align things perfectly without splitting points
    * and sorted.  We don't want to do that, so we have to allow for offsets.
    * In the end, the criterion is that any quantile range that spans a centroid
    * scale range more than one should be split across more than one centroid if
    * possible.  This won't be possible if the quantile range refers to a single point
    * or an already existing centroid.
    * <p/>
    * This mapping is steep near q=0 or q=1 so each centroid there will correspond to
    * less q range.  Near q=0.5, the mapping is flatter so that sorted there will
    * represent a larger chunk of quantiles.
    *
    * @param q The quantile scale value to be mapped.
    * @return The centroid scale value corresponding to q.
    */
    Value integratedLocation(Value q) const;
    Value integratedQ(Value k) const;

    /**
     * Same as {@link #weightedAverageSorted(Value, Value, Value, Value)} but flips
     * the order of the variables if <code>x2</code> is greater than
     * <code>x1</code>.
    */
    static Value weightedAverage(Value x1, Value w1, Value x2, Value w2);
    /**
    * Compute the weighted average between <code>x1</code> with a weight of
    * <code>w1</code> and <code>x2</code> with a weight of <code>w2</code>.
    * This expects <code>x1</code> to be less than or equal to <code>x2</code>
    * and is guaranteed to return a number between <code>x1</code> and
    * <code>x2</code>.
    */
    static Value weightedAverageSorted(Value x1, Value w1, Value x2, Value w2);
    static Value interpolate(Value x, Value x0, Value x1);
    /**
    * Computes an interpolated value of a quantile that is between two sorted.
    *
    * Index is the quantile desired multiplied by the total number of samples - 1.
    *
    * @param index              Denormalized quantile desired
    * @param previousIndex      The denormalized quantile corresponding to the center of the previous centroid.
    * @param nextIndex          The denormalized quantile corresponding to the center of the following centroid.
    * @param previousMean       The mean of the previous centroid.
    * @param nextMean           The mean of the following centroid.
    * @return  The interpolated mean.
    */
    static Value quantile(Value index, Value previousIndex, Value nextIndex, Value previousMean, Value nextMean);
};

} // namespace starrocks
