// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#pragma once

namespace starrocks::sstable {

class Iterator;

// Return an iterator that concatenates the data in children[0,n-1].
// Takes ownership of the child iterators and will delete them when
// the result iterator is deleted.
//
// REQUIRES: The keys in children[i] are all less than the keys in children[i+1]
// for all i in [0, n-2]. In other words, the files are already ordered
// and no merging is needed.
//
// REQUIRES: n >= 0
Iterator* NewConcatenatingIterator(Iterator** children, int n);

} // namespace starrocks::sstable
