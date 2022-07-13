
#include "storage/rowset/context_reader.h"

#include "column/chunk.h"
#include "column/column.h"
#include "exprs/expr_context.h"

namespace starrocks::vectorized {
Status ColumnCollector::seek_columns(ordinal_t pos) {
    for (auto iter : _column_iterators) {
        RETURN_IF_ERROR(iter->seek_to_ordinal(pos));
    }
    return Status::OK();
}

Status ColumnCollector::read_columns(Chunk* chunk, const vectorized::SparseRange& range) {
    bool may_has_del_row = chunk->delete_state() != DEL_NOT_SATISFIED;
    for (size_t i = 0; i < _column_iterators.size(); i++) {
        const ColumnPtr& col = chunk->get_column_by_index(i);
        RETURN_IF_ERROR(_column_iterators[i]->next_batch(range, col.get()));
        may_has_del_row |= (col->delete_state() != DEL_NOT_SATISFIED);
    }
    chunk->set_delete_state(may_has_del_row ? DEL_PARTIAL_SATISFIED : DEL_NOT_SATISFIED);
    return Status::OK();
}

} // namespace starrocks::vectorized