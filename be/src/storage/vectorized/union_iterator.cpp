// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/vectorized/union_iterator.h"

#include <memory>

#include "column/chunk.h"
#include "storage/iterators.h" // StorageReadOptions

namespace starrocks::vectorized {

class UnionIterator final : public ChunkIterator {
public:
    explicit UnionIterator(std::vector<ChunkIteratorPtr> children)
            : ChunkIterator(children[0]->schema(), children[0]->chunk_size()), _children(std::move(children)) {
#ifndef NDEBUG
        for (auto& iter : _children) {
            const Schema& child_schema = iter->schema();
            CHECK_EQ(_schema.num_fields(), child_schema.num_fields());
            for (int i = 0; i < _schema.num_fields(); i++) {
                CHECK_EQ(_schema.field(i)->to_string(), child_schema.field(i)->to_string());
            }
        }
#endif
    }

    ~UnionIterator() override = default;

    void close() override;

    size_t merged_rows() const override { return _merged_rows; }

    virtual Status init_res_schema(std::unordered_map<uint32_t, GlobalDictMap*>& dict_maps) override {
        ChunkIterator::init_res_schema(dict_maps);
        for (auto& child : _children) {
            child->init_res_schema(dict_maps);
        }
        return Status::OK();
    }

protected:
    Status do_get_next(Chunk* chunk) override;
    Status do_get_next(Chunk* chunk, std::vector<uint32_t>* rowid) override;

private:
    std::vector<ChunkIteratorPtr> _children;
    size_t _cur_idx = 0;
    size_t _merged_rows = 0;
};

inline Status UnionIterator::do_get_next(Chunk* chunk) {
    while (_cur_idx < _children.size()) {
        Status res = _children[_cur_idx]->get_next(chunk);
        if (res.is_end_of_file()) {
            _merged_rows += _children[_cur_idx]->merged_rows();
            _children[_cur_idx]->close();
            _children[_cur_idx].reset();
            _cur_idx++;
            continue;
        }
        return res;
    }
    return Status::EndOfFile("End of union iterator");
}

inline Status UnionIterator::do_get_next(Chunk* chunk, vector<uint32_t>* rowid) {
    while (_cur_idx < _children.size()) {
        Status res = _children[_cur_idx]->get_next(chunk, rowid);
        if (res.is_end_of_file()) {
            _merged_rows += _children[_cur_idx]->merged_rows();
            _children[_cur_idx]->close();
            _children[_cur_idx].reset();
            _cur_idx++;
            continue;
        }
        return res;
    }
    return Status::EndOfFile("End of union iterator");
}

inline void UnionIterator::close() {
    for (auto& ptr : _children) {
        if (ptr != nullptr) {
            ptr->close();
        }
    }
    _children.clear();
}

ChunkIteratorPtr new_union_iterator(std::vector<ChunkIteratorPtr> children) {
    DCHECK(!children.empty());
    if (children.size() == 1) {
        return children[0];
    }
    return std::make_shared<UnionIterator>(std::move(children));
}

} // namespace starrocks::vectorized
