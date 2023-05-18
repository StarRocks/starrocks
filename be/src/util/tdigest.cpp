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

#include "util/tdigest.h"

#include <algorithm>
#include <cmath>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include "common/logging.h"
#include "util/orlp/pdqsort.h"

namespace starrocks {

Centroid::Centroid() : Centroid(0.0, 0.0) {}

Centroid::Centroid(Value mean, Weight weight) : _mean(mean), _weight(weight) {}

Value Centroid::mean() const noexcept {
    return _mean;
}

Weight Centroid::weight() const noexcept {
    return _weight;
}

Value& Centroid::mean() noexcept {
    return _mean;
}

Weight& Centroid::weight() noexcept {
    return _weight;
}

void Centroid::add(const Centroid& c) {
    DCHECK_GT(c._weight, 0);
    if (_weight != 0.0) {
        _weight += c._weight;
        _mean += c._weight * (c._mean - _mean) / _weight;
    } else {
        _weight = c._weight;
        _mean = c._mean;
    }
}

TDigest::TDigest(Value compression, Index unmergedSize, Index mergedSize)
        : _compression(compression),
          _max_processed(processedSize(mergedSize, compression)),
          _max_unprocessed(unprocessedSize(unmergedSize, compression)) {
    // TODO: the performance of reserve
    // If the degree of aggregation is very low,each row records reserve (2000+8000) indexes,
    // which will occupy a large amount of system memory. Percentile itself is a CPU-heavy operation.
    // Reserve may have little effect on performance, so temporarily comment out these two. Line code,
    // and when a complete performance test shows that the impact on performance is relatively large,
    // redesign a new solution.

    //_processed.reserve(_max_processed);
    //_unprocessed.reserve(_max_unprocessed + 1);
}

TDigest::TDigest(std::vector<Centroid>&& processed, std::vector<Centroid>&& unprocessed, Value compression,
                 Index unmergedSize, Index mergedSize)
        : TDigest(compression, unmergedSize, mergedSize) {
    _processed = std::move(processed);
    _unprocessed = std::move(unprocessed);

    _processed_weight = weight(_processed);
    _unprocessed_weight = weight(_unprocessed);
    if (_processed.size() > 0) {
        _min = std::min(_min, _processed[0].mean());
        _max = std::max(_max, (_processed.cend() - 1)->mean());
    }
    updateCumulative();
}

Weight TDigest::weight(std::vector<Centroid>& centroids) noexcept {
    Weight w = 0.0;
    for (auto centroid : centroids) {
        w += centroid.weight();
    }
    return w;
}

Index TDigest::processedSize(Index size, Value compression) noexcept {
    return (size == 0) ? static_cast<Index>(2 * std::ceil(compression)) : size;
}

Index TDigest::unprocessedSize(Index size, Value compression) noexcept {
    return (size == 0) ? static_cast<Index>(8 * std::ceil(compression)) : size;
}

// merge in another t-digest
void TDigest::merge(const TDigest* other) {
    std::vector<const TDigest*> others{other};
    add(others.cbegin(), others.cend());
}

const std::vector<Centroid>& TDigest::processed() const {
    return _processed;
}

const std::vector<Centroid>& TDigest::unprocessed() const {
    return _unprocessed;
}

Index TDigest::maxUnprocessed() const {
    return _max_unprocessed;
}

Index TDigest::maxProcessed() const {
    return _max_processed;
}

void TDigest::add(const std::vector<const TDigest*>& digests) {
    add(digests.cbegin(), digests.cend());
}

void TDigest::add(std::vector<const TDigest*>::const_iterator iter, std::vector<const TDigest*>::const_iterator end) {
    if (iter != end) {
        auto size = std::distance(iter, end);
        TDigestQueue pq(TDigestComparator{});
        for (; iter != end; iter++) {
            pq.push((*iter));
        }
        std::vector<const TDigest*> batch;
        batch.reserve(size);

        size_t totalSize = 0;
        while (!pq.empty()) {
            auto td = pq.top();
            batch.push_back(td);
            pq.pop();
            totalSize += td->totalSize();
            if (totalSize >= kHighWater || pq.empty()) {
                mergeProcessed(batch);
                mergeUnprocessed(batch);
                processIfNecessary();
                batch.clear();
                totalSize = 0;
            }
        }
        updateCumulative();
    }
}

Weight TDigest::processedWeight() const {
    return _processed_weight;
}

Weight TDigest::unprocessedWeight() const {
    return _unprocessed_weight;
}

bool TDigest::haveUnprocessed() const {
    return _unprocessed.size() > 0;
}

size_t TDigest::totalSize() const {
    return _processed.size() + _unprocessed.size();
}

long TDigest::totalWeight() const {
    return static_cast<long>(_processed_weight + _unprocessed_weight);
}

Value TDigest::cdf(Value x) {
    if (haveUnprocessed() || isDirty()) process();
    return cdfProcessed(x);
}

bool TDigest::isDirty() {
    return _processed.size() > _max_processed || _unprocessed.size() > _max_unprocessed;
}

Value TDigest::cdfProcessed(Value x) const {
    VLOG(1) << "cdf value " << x;
    VLOG(1) << "processed size " << _processed.size();
    if (_processed.size() == 0) {
        // no data to examin_e
        VLOG(1) << "no processed values";

        return 0.0;
    } else if (_processed.size() == 1) {
        VLOG(1) << "one processed value "
                << " _min " << _min << " _max " << _max;
        // exactly one centroid, should have _max==_min
        auto width = _max - _min;
        if (x < _min) {
            return 0.0;
        } else if (x > _max) {
            return 1.0;
        } else if (x - _min <= width) {
            // _min and _max are too close together to do any viable interpolation
            return 0.5;
        } else {
            // interpolate if somehow we have weight > 0 and _max != _min
            return (x - _min) / (_max - _min);
        }
    } else {
        auto n = _processed.size();
        if (x <= _min) {
            VLOG(1) << "below _min "
                    << " _min " << _min << " x " << x;
            return 0;
        }

        if (x >= _max) {
            VLOG(1) << "above _max "
                    << " _max " << _max << " x " << x;
            return 1;
        }

        // check for the left tail
        if (x <= mean(0)) {
            VLOG(1) << "left tail "
                    << " _min " << _min << " mean(0) " << mean(0) << " x " << x;

            // note that this is different than mean(0) > _min ... this guarantees interpolation works
            if (mean(0) - _min > 0) {
                return (x - _min) / (mean(0) - _min) * weight(0) / _processed_weight / 2.0;
            } else {
                return 0;
            }
        }

        // and the right tail
        if (x >= mean(n - 1)) {
            VLOG(1) << "right tail"
                    << " _max " << _max << " mean(n - 1) " << mean(n - 1) << " x " << x;

            if (_max - mean(n - 1) > 0) {
                return 1.0 - (_max - x) / (_max - mean(n - 1)) * weight(n - 1) / _processed_weight / 2.0;
            } else {
                return 1;
            }
        }

        CentroidComparator cc;
        auto iter = std::upper_bound(_processed.cbegin(), _processed.cend(), Centroid(x, 0), cc);

        auto i = std::distance(_processed.cbegin(), iter);
        auto z1 = x - (iter - 1)->mean();
        auto z2 = (iter)->mean() - x;
        DCHECK_LE(0.0, z1);
        DCHECK_LE(0.0, z2);
        VLOG(1) << "middle "
                << " z1 " << z1 << " z2 " << z2 << " x " << x;

        return weightedAverage(_cumulative[i - 1], z2, _cumulative[i], z1) / _processed_weight;
    }
}

Value TDigest::quantile(Value q) {
    if (haveUnprocessed() || isDirty()) process();
    return quantileProcessed(q);
}

Value TDigest::quantileProcessed(Value q) const {
    if (q < 0 || q > 1) {
        VLOG(1) << "q should be in [0,1], got " << q;
        return NAN;
    }

    if (_processed.size() == 0) {
        // no sorted means no data, no way to get a quantile
        return NAN;
    } else if (_processed.size() == 1) {
        // with one data point, all quantiles lead to Rome

        return mean(0);
    }

    // we know that there are at least two sorted now
    auto n = _processed.size();

    // if values were stored in a sorted array, index would be the offset we are Weighterested in
    const auto index = q * _processed_weight;

    // at the boundaries, we return _min or _max
    if (index <= weight(0) / 2.0) {
        DCHECK_GT(weight(0), 0);
        return _min + 2.0 * index / weight(0) * (mean(0) - _min);
    }

    auto iter = std::lower_bound(_cumulative.cbegin(), _cumulative.cend(), index);

    if (iter + 1 != _cumulative.cend()) {
        auto i = std::distance(_cumulative.cbegin(), iter);
        auto z1 = index - *(iter - 1);
        auto z2 = *(iter)-index;
        // VLOG(1) << "z2 " << z2 << " index " << index << " z1 " << z1;
        return weightedAverage(mean(i - 1), z2, mean(i), z1);
    }

    DCHECK_LE(index, _processed_weight);
    DCHECK_GE(index, _processed_weight - weight(n - 1) / 2.0);

    auto z1 = index - _processed_weight - weight(n - 1) / 2.0;
    auto z2 = weight(n - 1) / 2 - z1;
    return weightedAverage(mean(n - 1), z1, _max, z2);
}

Value TDigest::compression() const {
    return _compression;
}

void TDigest::add(Value x) {
    add(x, 1);
}

void TDigest::compress() {
    process();
}

bool TDigest::add(Value x, Weight w) {
    if (std::isnan(x)) {
        return false;
    }
    _unprocessed.emplace_back(x, w);
    _unprocessed_weight += w;
    processIfNecessary();
    return true;
}

void TDigest::add(std::vector<Centroid>::const_iterator iter, std::vector<Centroid>::const_iterator end) {
    while (iter != end) {
        const size_t diff = std::distance(iter, end);
        const size_t room = _max_unprocessed - _unprocessed.size();
        auto mid = iter + std::min(diff, room);
        while (iter != mid) _unprocessed.push_back(*(iter++));
        if (_unprocessed.size() >= _max_unprocessed) {
            process();
        }
    }
}

uint64_t TDigest::serialize_size() const {
    return sizeof(Value) * 5 + sizeof(Index) * 2 + sizeof(size_t) * 3 + _processed.size() * sizeof(Centroid) +
           _unprocessed.size() * sizeof(Centroid) + _cumulative.size() * sizeof(Weight);
}

size_t TDigest::serialize(uint8_t* writer) const {
    memcpy(writer, &_compression, sizeof(Value));
    writer += sizeof(Value);
    memcpy(writer, &_min, sizeof(Value));
    writer += sizeof(Value);
    memcpy(writer, &_max, sizeof(Value));
    writer += sizeof(Value);
    memcpy(writer, &_max_processed, sizeof(Index));
    writer += sizeof(Index);
    memcpy(writer, &_max_unprocessed, sizeof(Index));
    writer += sizeof(Index);
    memcpy(writer, &_processed_weight, sizeof(Value));
    writer += sizeof(Value);
    memcpy(writer, &_unprocessed_weight, sizeof(Value));
    writer += sizeof(Value);

    uint32_t size = _processed.size();
    memcpy(writer, &size, sizeof(uint32_t));
    writer += sizeof(uint32_t);
    for (int i = 0; i < size; i++) {
        memcpy(writer, &_processed[i], sizeof(Centroid));
        writer += sizeof(Centroid);
    }

    size = _unprocessed.size();
    memcpy(writer, &size, sizeof(uint32_t));
    writer += sizeof(uint32_t);
    for (int i = 0; i < size; i++) {
        memcpy(writer, &_unprocessed[i], sizeof(Centroid));
        writer += sizeof(Centroid);
    }

    size = _cumulative.size();
    memcpy(writer, &size, sizeof(uint32_t));
    writer += sizeof(uint32_t);
    for (int i = 0; i < size; i++) {
        memcpy(writer, &_cumulative[i], sizeof(Weight));
        writer += sizeof(Weight);
    }

    return serialize_size();
}

void TDigest::deserialize(const char* type_reader) {
    memcpy(&_compression, type_reader, sizeof(Value));
    type_reader += sizeof(Value);
    memcpy(&_min, type_reader, sizeof(Value));
    type_reader += sizeof(Value);
    memcpy(&_max, type_reader, sizeof(Value));
    type_reader += sizeof(Value);

    memcpy(&_max_processed, type_reader, sizeof(Index));
    type_reader += sizeof(Index);
    memcpy(&_max_unprocessed, type_reader, sizeof(Index));
    type_reader += sizeof(Index);
    memcpy(&_processed_weight, type_reader, sizeof(Value));
    type_reader += sizeof(Value);
    memcpy(&_unprocessed_weight, type_reader, sizeof(Value));
    type_reader += sizeof(Value);

    uint32_t size;
    memcpy(&size, type_reader, sizeof(uint32_t));
    type_reader += sizeof(uint32_t);
    _processed.resize(size);
    for (int i = 0; i < size; i++) {
        memcpy(&_processed[i], type_reader, sizeof(Centroid));
        type_reader += sizeof(Centroid);
    }
    memcpy(&size, type_reader, sizeof(uint32_t));
    type_reader += sizeof(uint32_t);
    _unprocessed.resize(size);
    for (int i = 0; i < size; i++) {
        memcpy(&_unprocessed[i], type_reader, sizeof(Centroid));
        type_reader += sizeof(Centroid);
    }
    memcpy(&size, type_reader, sizeof(uint32_t));
    type_reader += sizeof(uint32_t);
    _cumulative.resize(size);
    for (int i = 0; i < size; i++) {
        memcpy(&_cumulative[i], type_reader, sizeof(Weight));
        type_reader += sizeof(Weight);
    }
}

Value TDigest::mean(int i) const noexcept {
    return _processed[i].mean();
}

Weight TDigest::weight(int i) const noexcept {
    return _processed[i].weight();
}

void TDigest::mergeUnprocessed(const std::vector<const TDigest*>& tdigests) {
    if (tdigests.size() == 0) return;

    size_t total = _unprocessed.size();
    for (auto& td : tdigests) {
        total += td->_unprocessed.size();
    }

    _unprocessed.reserve(total);
    for (auto& td : tdigests) {
        _unprocessed.insert(_unprocessed.end(), td->_unprocessed.cbegin(), td->_unprocessed.cend());
        _unprocessed_weight += td->_unprocessed_weight;
    }
}

void TDigest::mergeProcessed(const std::vector<const TDigest*>& tdigests) {
    if (tdigests.size() == 0) return;

    size_t total = 0;
    CentroidListQueue pq(CentroidListComparator{});
    for (auto& td : tdigests) {
        auto& sorted = td->_processed;
        auto size = sorted.size();
        if (size > 0) {
            pq.push(CentroidList(sorted));
            total += size;
            _processed_weight += td->_processed_weight;
        }
    }
    if (total == 0) return;

    if (_processed.size() > 0) {
        pq.push(CentroidList(_processed));
        total += _processed.size();
    }

    std::vector<Centroid> sorted;
    VLOG(1) << "total " << total;
    sorted.reserve(total);

    while (!pq.empty()) {
        auto best = pq.top();
        pq.pop();
        sorted.push_back(*(best.iter));
        if (best.advance()) pq.push(best);
    }
    _processed = std::move(sorted);
    if (_processed.size() > 0) {
        _min = std::min(_min, _processed[0].mean());
        _max = std::max(_max, (_processed.cend() - 1)->mean());
    }
}

void TDigest::processIfNecessary() {
    if (isDirty()) {
        process();
    }
}

void TDigest::updateCumulative() {
    const auto n = _processed.size();
    _cumulative.clear();
    _cumulative.reserve(n + 1);
    auto previous = 0.0;
    for (Index i = 0; i < n; i++) {
        auto current = weight(i);
        auto halfCurrent = current / 2.0;
        _cumulative.push_back(previous + halfCurrent);
        previous = previous + current;
    }
    _cumulative.push_back(previous);
}

void TDigest::process() {
    CentroidComparator cc;
    pdqsort(_unprocessed.begin(), _unprocessed.end(), [&](auto& lhs, auto& rhs) { return lhs.mean() < rhs.mean(); });
    auto count = _unprocessed.size();
    _unprocessed.insert(_unprocessed.end(), _processed.cbegin(), _processed.cend());
    std::inplace_merge(_unprocessed.begin(), _unprocessed.begin() + count, _unprocessed.end(), cc);

    _processed_weight += _unprocessed_weight;
    _unprocessed_weight = 0;
    _processed.clear();

    _processed.push_back(_unprocessed[0]);
    Weight wSoFar = _unprocessed[0].weight();
    Weight wLimit = _processed_weight * integratedQ(1.0);

    auto end = _unprocessed.end();
    for (auto iter = _unprocessed.cbegin() + 1; iter < end; iter++) {
        auto& centroid = *iter;
        Weight projectedW = wSoFar + centroid.weight();
        if (projectedW <= wLimit) {
            wSoFar = projectedW;
            (_processed.end() - 1)->add(centroid);
        } else {
            auto k1 = integratedLocation(wSoFar / _processed_weight);
            wLimit = _processed_weight * integratedQ(k1 + 1.0);
            wSoFar += centroid.weight();
            _processed.emplace_back(centroid);
        }
    }
    _unprocessed.clear();
    _min = std::min(_min, _processed[0].mean());
    VLOG(1) << "new _min " << _min;
    _max = std::max(_max, (_processed.cend() - 1)->mean());
    VLOG(1) << "new _max " << _max;
    updateCumulative();
}

int TDigest::checkWeights() {
    return checkWeights(_processed, _processed_weight);
}

size_t TDigest::checkWeights(const std::vector<Centroid>& sorted, Value total) {
    size_t badWeight = 0;
    auto k1 = 0.0;
    auto q = 0.0;
    for (auto iter = sorted.cbegin(); iter != sorted.cend(); iter++) {
        auto w = iter->weight();
        auto dq = w / total;
        auto k2 = integratedLocation(q + dq);
        if (k2 - k1 > 1 && w != 1) {
            VLOG(1) << "Oversize centroid at " << std::distance(sorted.cbegin(), iter) << " k1 " << k1 << " k2 " << k2
                    << " dk " << (k2 - k1) << " w " << w << " q " << q;
            badWeight++;
        }
        if (k2 - k1 > 1.5 && w != 1) {
            VLOG(1) << "Egregiously Oversize centroid at " << std::distance(sorted.cbegin(), iter) << " k1 " << k1
                    << " k2 " << k2 << " dk " << (k2 - k1) << " w " << w << " q " << q;
            badWeight++;
        }
        q += dq;
        k1 = k2;
    }

    return badWeight;
}

Value TDigest::integratedLocation(Value q) const {
    return _compression * (std::asin(2.0 * q - 1.0) + M_PI / 2) / M_PI;
}

Value TDigest::integratedQ(Value k) const {
    return (std::sin(std::min(k, _compression) * M_PI / _compression - M_PI / 2) + 1) / 2;
}

Value TDigest::weightedAverage(Value x1, Value w1, Value x2, Value w2) {
    return (x1 <= x2) ? weightedAverageSorted(x1, w1, x2, w2) : weightedAverageSorted(x2, w2, x1, w1);
}

Value TDigest::weightedAverageSorted(Value x1, Value w1, Value x2, Value w2) {
    DCHECK_LE(x1, x2);
    const Value x = (x1 * w1 + x2 * w2) / (w1 + w2);
    return std::max(x1, std::min(x, x2));
}

Value TDigest::interpolate(Value x, Value x0, Value x1) {
    return (x - x0) / (x1 - x0);
}

Value TDigest::quantile(Value index, Value previousIndex, Value nextIndex, Value previousMean, Value nextMean) {
    const auto delta = nextIndex - previousIndex;
    const auto previousWeight = (nextIndex - index) / delta;
    const auto nextWeight = (index - previousIndex) / delta;
    return previousMean * previousWeight + nextMean * nextWeight;
}

} // namespace starrocks
