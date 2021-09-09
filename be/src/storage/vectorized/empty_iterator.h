// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/chunk_iterator.h"

namespace starrocks::vectorized {

ChunkIteratorPtr new_empty_iterator(vectorized::Schema&& schema, int chunk_size);
ChunkIteratorPtr new_empty_iterator(const vectorized::Schema& schema, int chunk_size);

} // namespace starrocks::vectorized
