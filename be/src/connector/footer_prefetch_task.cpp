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

#include "connector/footer_prefetch_task.h"

#include "cache/datacache.h"
#include "cache/scan/cache_input_stream.h"
#include "exec/hdfs_scanner/hdfs_scanner.h"
#include "formats/parquet/metadata.h"

namespace starrocks::connector {

std::string footer_prefetch_key(const TScanRange& scan_range) {
    const auto& hdfs = scan_range.hdfs_scan_range;
    if (!hdfs.full_path.empty()) {
        return hdfs.full_path;
    }
    return std::to_string(hdfs.partition_id) + ":" + hdfs.relative_path;
}

FooterWarmResult warm_footer(const pipeline::FooterPrefetchItem& item, bool metacache_on) {
    FooterWarmResult result;
    const auto* octx = item.open_ctx.get();
    if (octx == nullptr || octx->fs == nullptr || item.file_size <= 0) {
        return result;
    }

    // modification_time is per file (it is part of the cache key); the shared context carries
    // the rest of the datacache policy as a template.
    DataCacheOptions datacache_options = octx->datacache_options;
    datacache_options.modification_time = item.modification_time;

    // Throwaway stats: the footer read/parse path writes counters into the scanner context.
    HdfsScanStats stats;
    OpenFileOptions opts;
    opts.fs = octx->fs.get();
    opts.path = item.path;
    opts.file_size = item.file_size;
    opts.fs_stats = &stats;
    opts.app_stats = &stats;
    opts.datacache_options = datacache_options;

    // Open the file the same cache-wrapped way the real scan does: when datacache+populate
    // are on, reads through this stream populate BlockCache (the normal-scan path).
    std::shared_ptr<SharedBufferedInputStream> sb_stream;
    std::shared_ptr<CacheInputStream> cache_stream;
    auto file_or = HdfsScanner::create_random_access_file(sb_stream, cache_stream, opts);
    if (!file_or.ok()) {
        return result; // swallow: prefetch must never poison the real scan
    }
    std::unique_ptr<RandomAccessFile> file = std::move(file_or.value());

    // get_file_metadata reads the footer tail through the cache-wrapped stream (populating
    // BlockCache when datacache populate is on) and, when a page cache is supplied, parses and
    // inserts the FileMetaData into PageCache. A null page cache (metacache off) warms BlockCache
    // only; a cache hit warms nothing.
    HdfsScannerContext ctx;
    ctx.stats = &stats;
    ctx.case_sensitive = octx->case_sensitive;
    ctx.split_context = nullptr;
    ctx.lazy_column_coalesce_counter = nullptr; // unused by get_file_metadata; avoid an indeterminate pointer
    StoragePageCache* page_cache = metacache_on ? DataCache::GetInstance()->page_cache() : nullptr;
    parquet::FileMetaDataParser parser(file.get(), &ctx, page_cache, &datacache_options, item.file_size);
    (void)parser.get_file_metadata(); // discard; warming is the only goal

    // Count actual writes (a cache hit warms nothing). PageCache write == footer was parsed and
    // inserted; BlockCache write == the wrapped read populated block(s).
    result.wrote_pagecache = stats.footer_cache_write_count > 0;
    result.wrote_blockcache = cache_stream != nullptr && cache_stream->stats().write_block_cache_count > 0;
    return result;
}

} // namespace starrocks::connector
