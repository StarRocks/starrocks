// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#include "storage/lake/sstable/options.h"

#include "storage/lake/sstable/comparator.h"

namespace starrocks {

namespace lake {

namespace sstable {

Options::Options() : comparator(BytewiseComparator()) {}

} // namespace sstable
} // namespace lake
} // namespace starrocks