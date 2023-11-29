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
#include "CLucene.h"
#include "roaring/roaring.hh"

namespace starrocks {
class RoaringHitCollector final : public lucene::search::HitCollector {
public:
    RoaringHitCollector(roaring::Roaring* bitmap) : _collector(bitmap){};
    void collect(const int32_t doc, const float_t score) override { _collector->add(doc); }

private:
    roaring::Roaring* _collector;
};

class RoaringHitWithScoreCollector final : public lucene::search::HitCollector {
public:
    RoaringHitWithScoreCollector(roaring::Roaring* bitmap, std::vector<float_t>* scores)
            : _collector(bitmap), _scores(scores){};
    void collect(const int32_t doc, const float_t score) override {
        _collector->add(doc);
        _scores->emplace_back(score);
    }

private:
    roaring::Roaring* _collector;
    std::vector<float_t>* _scores;
};
} // namespace starrocks