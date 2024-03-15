// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#pragma once

#include <string>

namespace starrocks {
class Cache;

namespace sstable {

class Comparator;
class FilterPolicy;
// DB contents are stored in a set of blocks, each of which holds a
// sequence of key,value pairs.  Each block may be compressed before
// being stored in a file.  The following enum describes which
// compression method (if any) is used to compress a block.
enum CompressionType {
    // NOTE: do not change the values of existing entries, as these are
    // part of the persistent format on disk.
    kNoCompression = 0x0,
    kSnappyCompression = 0x1
};

// Options to control the behavior of a database (passed to DB::Open)
struct Options {
    // Create an Options object with default values for all fields.
    Options();

    // -------------------
    // Parameters that affect behavior

    // Comparator used to define the order of keys in the table.
    // Default: a comparator that uses lexicographic byte-wise ordering
    //
    // REQUIRES: The client must ensure that the comparator supplied
    // here has the same name and orders keys *exactly* the same as the
    // comparator provided to previous open calls on the same DB.
    const Comparator* comparator;

    // If true, the implementation will do aggressive checking of the
    // data it is processing and will stop early if it detects any
    // errors.  This may have unforeseen ramifications: for example, a
    // corruption of one DB entry may cause a large number of entries to
    // become unreadable or for the entire DB to become unopenable.
    bool paranoid_checks = false;

    // If non-null, use the specified cache for blocks.
    // If null, leveldb will automatically create and use an 8MB internal cache.
    Cache* block_cache = nullptr;

    // Approximate size of user data packed per block.  Note that the
    // block size specified here corresponds to uncompressed data.  The
    // actual size of the unit read from disk may be smaller if
    // compression is enabled.  This parameter can be changed dynamically.
    size_t block_size = 4 * 1024;

    // Number of keys between restart points for delta encoding of keys.
    // This parameter can be changed dynamically.  Most clients should
    // leave this parameter alone.
    int block_restart_interval = 16;

    // Leveldb will write up to this amount of bytes to a file before
    // switching to a new one.
    // Most clients should leave this parameter alone.  However if your
    // filesystem is more efficient with larger files, you could
    // consider increasing the value.  The downside will be longer
    // compactions and hence longer latency/performance hiccups.
    // Another reason to increase this parameter might be when you are
    // initially populating a large database.
    size_t max_file_size = 2 * 1024 * 1024;

    // Compress blocks using the specified compression algorithm.  This
    // parameter can be changed dynamically.
    //
    // Default: kSnappyCompression, which gives lightweight but fast
    // compression.
    //
    // Typical speeds of kSnappyCompression on an Intel(R) Core(TM)2 2.4GHz:
    //    ~200-500MB/s compression
    //    ~400-800MB/s decompression
    // Note that these speeds are significantly faster than most
    // persistent storage speeds, and therefore it is typically never
    // worth switching to kNoCompression.  Even if the input data is
    // incompressible, the kSnappyCompression implementation will
    // efficiently detect that and will switch to uncompressed mode.
    CompressionType compression = kSnappyCompression;

    // If non-null, use the specified filter policy to reduce disk reads.
    // Many applications will benefit from passing the result of
    // NewBloomFilterPolicy() here.
    const FilterPolicy* filter_policy = nullptr;
};

struct ReadIOStat {
    ReadIOStat() = default;

    // Bytes read from files
    uint64_t bytes_from_file = 0;

    // Bytes read from page cache
    uint64_t bytes_from_cache = 0;

    // Read block cnt from files
    uint32_t block_cnt_from_file = 0;

    // Read block cnt from page cache
    uint32_t block_cnt_from_cache = 0;
};

// Options that control read operations
struct ReadOptions {
    ReadOptions() = default;

    // If true, all data read from underlying storage will be
    // verified against corresponding checksums.
    bool verify_checksums = false;

    // Should the data read for this iteration be cached in memory?
    // Callers may wish to set this field to false for bulk scans.
    bool fill_cache = true;

    ReadIOStat* stat = nullptr;
};

// Options that control write operations
struct WriteOptions {
    WriteOptions() = default;

    // If true, the write will be flushed from the operating system
    // buffer cache (by calling WritableFile::Sync()) before the write
    // is considered complete.  If this flag is true, writes will be
    // slower.
    //
    // If this flag is false, and the machine crashes, some recent
    // writes may be lost.  Note that if it is just the process that
    // crashes (i.e., the machine does not reboot), no writes will be
    // lost even if sync==false.
    //
    // In other words, a DB write with sync==false has similar
    // crash semantics as the "write()" system call.  A DB write
    // with sync==true has similar crash semantics to a "write()"
    // system call followed by "fsync()".
    bool sync = false;
};

} // namespace sstable
} // namespace starrocks
