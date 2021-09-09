// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "storage/vectorized/chunk_iterator.h"

namespace starrocks::vectorized {
ChunkIteratorPtr new_aggregate_iterator(ChunkIteratorPtr child, int factor = 0);
}
