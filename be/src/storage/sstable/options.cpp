// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/sstable/options.h"

#include "storage/sstable/comparator.h"

namespace starrocks::sstable {

Options::Options() : comparator(BytewiseComparator()) {}

} // namespace starrocks::sstable