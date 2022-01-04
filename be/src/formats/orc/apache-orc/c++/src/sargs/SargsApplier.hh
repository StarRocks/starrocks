// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/src/sargs/SargsApplier.hh

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include <orc/Common.hh>
#include <unordered_map>

#include "orc/BloomFilter.hh"
#include "orc/Type.hh"
#include "sargs/SearchArgument.hh"
#include "wrap/orc-proto-wrapper.hh"

namespace orc {
class SargsApplier {
public:
    SargsApplier(const Type& type, const SearchArgument* searchArgument, uint64_t rowIndexStride,
                 WriterVersion writerVersion);

    SargsApplier(const Type& type, const SearchArgument* searchArgument, RowReaderFilter* RowReaderFilter,
                 uint64_t rowIndexStride, WriterVersion writerVersion);

    /**
     * TODO: use proto::RowIndex and proto::BloomFilter to do the evaluation
     * Pick the row groups that we need to load from the current stripe.
     * @return true if any row group is selected
     */
    bool pickRowGroups(uint64_t rowsInStripe, const std::unordered_map<uint64_t, proto::RowIndex>& rowIndexes,
                       const std::map<uint32_t, BloomFilterIndex>& bloomFilters);

    /**
     * Return a vector of bool for each row group for their selection
     * in the last evaluation
     */
    const std::vector<bool>& getRowGroups() const { return mRowGroups; }

    /**
     * Indicate whether any row group is selected in the last evaluation
     */
    bool hasSelected() const { return mHasSelected; }

    /**
     * Indicate whether any row group is skipped in the last evaluation
     */
    bool hasSkipped() const { return mHasSkipped; }

    /**
     * Whether any row group from current row in the stripe matches PPD.
     */
    bool hasSelectedFrom(uint64_t currentRowInStripe) const {
        uint64_t rg = currentRowInStripe / mRowIndexStride;
        for (; rg < mRowGroups.size(); ++rg) {
            if (mRowGroups[rg]) {
                return true;
            }
        }
        return false;
    }

    std::pair<uint64_t, uint64_t> getStats() const { return mStats; }

    RowReaderFilter* getRowReaderFilter() const { return mRowReaderFilter; }

private:
    friend class TestSargsApplier_findColumnTest_Test;
    static uint64_t findColumn(const Type& type, const std::string& colName);

private:
    const Type& mType;
    const SearchArgument* mSearchArgument;
    RowReaderFilter* mRowReaderFilter;
    uint64_t mRowIndexStride;
    WriterVersion mWriterVersion;
    // column ids for each predicate leaf in the search argument
    std::vector<uint64_t> mFilterColumns;

    // store results of last call of pickRowGroups
    std::vector<bool> mRowGroups;
    uint64_t mTotalRowsInStripe;
    bool mHasSelected;
    bool mHasSkipped;
    // keep stats of selected RGs and evaluated RGs
    std::pair<uint64_t, uint64_t> mStats;
};

} // namespace orc
