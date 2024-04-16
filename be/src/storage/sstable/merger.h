// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license.
// (https://developers.google.com/open-source/licenses/bsd)

#pragma once

#include <vector>

namespace starrocks::sstable {

class Comparator;
class Iterator;

// Return an iterator that provided the union of the data in
// children[0,n-1].  Takes ownership of the child iterators and
// will delete them when the result iterator is deleted.
//
// The result does no duplicate suppression.  I.e., if a particular
// key is present in K child iterators, it will be yielded K times.
//
// REQUIRES: childrens.size() == versions.size()
Iterator* NewMergingIterator(const Comparator* comparator, const std::vector<Iterator*>& childrens,
                             const std::vector<int64_t>& versions);

} // namespace starrocks::sstable
