// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)
#pragma once

#include "common/status.h"

namespace starrocks {
struct KeyIndexesInfo;
class Slice;
class RandomAccessFile;
namespace sstable {
class Block;
class BlockHandle;
class Footer;
struct Options;
struct ReadOptions;
class Iterator;

// A Table is a sorted map from strings to strings.  Tables are
// immutable and persistent.  A Table may be safely accessed from
// multiple threads without external synchronization.
class Table {
public:
    // Attempt to open the table that is stored in bytes [0..file_size)
    // of "file", and read the metadata entries necessary to allow
    // retrieving data from the table.
    //
    // If successful, returns ok and sets "*table" to the newly opened
    // table.  The client should delete "*table" when no longer needed.
    // If there was an error while initializing the table, sets "*table"
    // to nullptr and returns a non-ok status.  Does not take ownership of
    // "*source", but the client must ensure that "source" remains live
    // for the duration of the returned table's lifetime.
    //
    // *file must remain live while this Table is in use.
    static Status Open(const Options& options, RandomAccessFile* file, uint64_t file_size, Table** table);

    Table(const Table&) = delete;
    Table& operator=(const Table&) = delete;

    ~Table();

    // Returns a new iterator over the table contents.
    // The result of NewIterator() is initially invalid (caller must
    // call one of the Seek methods on the iterator before using it).
    Iterator* NewIterator(const ReadOptions&) const;

    // Calls (*handle_result)(arg, ...) with the entry found after a call
    // to Seek(key).  May not make such a call if filter policy says
    // that key is not present.
    Status MultiGet(const ReadOptions&, size_t n, const Slice* keys, const KeyIndexesInfo& keys_info,
                    std::vector<std::string>* values);

private:
    struct Rep;

    static Iterator* BlockReader(void*, const ReadOptions&, const Slice&);

    explicit Table(Rep* rep) : rep_(rep) {}

    void ReadMeta(const Footer& footer);
    void ReadFilter(const Slice& filter_handle_value);

    Rep* const rep_;
};
} // namespace sstable
} // namespace starrocks
