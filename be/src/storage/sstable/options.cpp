// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/sstable/options.h"

#include "storage/sstable/comparator.h"

namespace starrocks::sstable {

Options::Options() : comparator(BytewiseComparator()) {}

void Options::set_compression(CompressionTypePB type) {
    if (type == CompressionTypePB::LZ4_FRAME || type == CompressionTypePB::LZ4) {
        compression = kLz4FrameCompression;
    } else if (type == CompressionTypePB::ZSTD) {
        compression = kZstdCompression;
    } else {
        // use default snappy.
    }
}

} // namespace starrocks::sstable