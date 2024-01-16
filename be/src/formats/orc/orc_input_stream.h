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

#include <exec/hdfs_scanner.h>

#include <boost/algorithm/string.hpp>
#include <orc/OrcFile.hh>

#include "exprs/expr.h"
#include "exprs/expr_context.h"
#include "exprs/runtime_filter_bank.h"
#include "io/shared_buffered_input_stream.h"
namespace starrocks {

class RandomAccessFile;

class ORCHdfsFileStream : public orc::InputStream {
public:
    struct StripeInformation {
        uint64_t offset;
        uint64_t length;
    };

    // |file| must outlive ORCHdfsFileStream
    ORCHdfsFileStream(RandomAccessFile* file, uint64_t length, io::SharedBufferedInputStream* sb_stream);

    ~ORCHdfsFileStream() override = default;

    uint64_t getLength() const override { return _length; }

    // refers to paper `Delta Lake: High-Performance ACID Table Storage over Cloud Object Stores`
    uint64_t getNaturalReadSize() const override { return config::orc_natural_read_size; }

    // It's for read size after doing seek.
    // When doing read after seek, we make assumption that we are doing random read because of seeking row group.
    // And if we still use NaturalReadSize we probably read many row groups
    // after the row group we want to read, and that will amplify read IO bytes.

    // So the best way is to reduce read size, hopefully we just read that row group in one shot.
    // We also have chance that we may not read enough at this shot, then we fallback to NaturalReadSize to read.
    // The cost is, there is a extra IO, and we read 1/4 of NaturalReadSize more data.
    // And the potential gain is, we save 3/4 of NaturalReadSize IO bytes.

    // Normally 256K can cover a row group of a column(like integer or double, but maybe not string)
    // And this value can not be too small because if we can not read a row group in a single shot,
    // we will fallback to read in normal size, and we pay cost of a extra read.

    uint64_t getNaturalReadSizeAfterSeek() const override { return config::orc_natural_read_size / 4; }

    void prepareCache(PrepareCacheScope scope, uint64_t offset, uint64_t length) override;
    void read(void* buf, uint64_t length, uint64_t offset) override;

    const std::string& getName() const override;

    void set_lazy_column_coalesce_counter(const std::atomic<int32_t>* lazy_column_coalesce_counter) {
        _lazy_column_coalesce_counter = lazy_column_coalesce_counter;
    }
    void set_app_stats(HdfsScanStats* stats) { _app_stats = stats; }
    bool isIOCoalesceEnabled() const override { return config::orc_coalesce_read_enable; }
    bool isIOAdaptiveCoalesceEnabled() const override { return config::io_coalesce_adaptive_lazy_active; }

    void clearIORanges() override;
    void setIORanges(std::vector<IORange>& io_ranges, const bool is_from_stripe) override;
    void setStripes(std::vector<StripeInformation>&& stripes);

private:
    void doRead(void* buf, uint64_t length, uint64_t offset);
    bool isAlreadyCachedInBuffer(uint64_t offset, uint64_t length);
    uint64_t computeCacheFullStripeSize(uint64_t offset, uint64_t length);

    RandomAccessFile* _file;
    uint64_t _length;
    std::vector<char> _cache_buffer;
    uint64_t _cache_offset;
    io::SharedBufferedInputStream* _sb_stream;

    bool _tiny_stripe_read = false;
    uint64_t _last_stripe_index = 0;
    std::vector<StripeInformation> _stripes;

    const std::atomic<int32_t>* _lazy_column_coalesce_counter = nullptr;
    HdfsScanStats* _app_stats = nullptr;
};
} // namespace starrocks
