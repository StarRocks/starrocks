// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/orc/tree/main/c++/src/sargs/SargsApplier.cc

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

#include "SargsApplier.hh"

#include <numeric>

namespace orc {

// find column id from column name
uint64_t SargsApplier::findColumn(const Type& type, const std::string& colName) {
    for (uint64_t i = 0; i != type.getSubtypeCount(); ++i) {
        // Only STRUCT type has field names
        if (type.getKind() == STRUCT && i < type.getFieldNamesCount() && type.getFieldName(i) == colName) {
            return type.getSubtype(i)->getColumnId();
        } else {
            uint64_t ret = findColumn(*type.getSubtype(i), colName);
            if (ret != INVALID_COLUMN_ID) {
                return ret;
            }
        }
    }
    return INVALID_COLUMN_ID;
}

SargsApplier::SargsApplier(const Type& type, const SearchArgument* searchArgument, uint64_t rowIndexStride,
                           WriterVersion writerVersion)
        : SargsApplier(type, searchArgument, nullptr, rowIndexStride, writerVersion) {}

SargsApplier::SargsApplier(const Type& type, const SearchArgument* searchArgument, RowReaderFilter* rowReaderFilter,
                           uint64_t rowIndexStride, WriterVersion writerVersion)
        : mType(type),
          mSearchArgument(searchArgument),
          mRowReaderFilter(rowReaderFilter),
          mRowIndexStride(rowIndexStride),
          mWriterVersion(writerVersion),
          mStats(0, 0) {
    const SearchArgumentImpl* sargs = dynamic_cast<const SearchArgumentImpl*>(mSearchArgument);

    // find the mapping from predicate leaves to columns
    const std::vector<PredicateLeaf>& leaves = sargs->getLeaves();
    mFilterColumns.resize(leaves.size(), INVALID_COLUMN_ID);
    for (size_t i = 0; i != mFilterColumns.size(); ++i) {
        mFilterColumns[i] = findColumn(type, leaves[i].getColumnName());
    }
}

bool SargsApplier::pickRowGroups(uint64_t rowsInStripe, const std::unordered_map<uint64_t, proto::RowIndex>& rowIndexes,
                                 const std::map<uint32_t, BloomFilterIndex>& bloomFilters) {
    // init state of each row group
    uint64_t groupsInStripe = (rowsInStripe + mRowIndexStride - 1) / mRowIndexStride;
    mRowGroups.resize(groupsInStripe, true);
    mTotalRowsInStripe = rowsInStripe;

    // row indexes do not exist, simply read all rows
    if (rowIndexes.empty()) {
        return true;
    }

    if (mRowReaderFilter) {
        mRowReaderFilter->onStartingPickRowGroups();
    }

    const auto& leaves = dynamic_cast<const SearchArgumentImpl*>(mSearchArgument)->getLeaves();
    std::vector<TruthValue> leafValues(leaves.size(), TruthValue::YES_NO_NULL);
    mHasSelected = false;
    mHasSkipped = false;
    for (size_t rowGroup = 0; rowGroup != groupsInStripe; ++rowGroup) {
        for (size_t pred = 0; pred != leaves.size(); ++pred) {
            uint64_t columnIdx = mFilterColumns[pred];
            auto rowIndexIter = rowIndexes.find(columnIdx);
            if (columnIdx == INVALID_COLUMN_ID || rowIndexIter == rowIndexes.cend()) {
                // this column does not exist in current file
                leafValues[pred] = TruthValue::YES_NO_NULL;
            } else {
                // get column statistics
                const proto::ColumnStatistics& statistics =
                        rowIndexIter->second.entry(static_cast<int>(rowGroup)).statistics();

                // get bloom filter
                std::shared_ptr<BloomFilter> bloomFilter;
                auto iter = bloomFilters.find(static_cast<uint32_t>(columnIdx));
                if (iter != bloomFilters.cend()) {
                    bloomFilter = iter->second.entries.at(rowGroup);
                }

                leafValues[pred] = leaves[pred].evaluate(mWriterVersion, statistics, bloomFilter.get());
            }
        }
        mRowGroups[rowGroup] = isNeeded(mSearchArgument->evaluate(leafValues));

        // I guess cost of evaluating search argument is lower than our customized filter.
        // so better to put it ahead of our customized filter.
        if (mRowReaderFilter && mRowGroups[rowGroup] &&
            mRowReaderFilter->filterOnPickRowGroup(rowGroup, rowIndexes, bloomFilters)) {
            mRowGroups[rowGroup] = false;
        }

        mHasSelected = mHasSelected || mRowGroups[rowGroup];
        mHasSkipped = mHasSkipped || (!mRowGroups[rowGroup]);
    }

    if (mRowReaderFilter) {
        mRowReaderFilter->onEndingPickRowGroups();
    }

    // update stats
    mStats.first = std::accumulate(mRowGroups.cbegin(), mRowGroups.cend(), mStats.first,
                                   [](bool rg, uint64_t s) { return rg ? 1 : 0 + s; });
    mStats.second += groupsInStripe;

    return mHasSelected;
}

} // namespace orc
