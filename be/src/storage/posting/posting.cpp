#include "posting.h"

#include "storage/posting/encoder.h"

namespace starrocks {

PostingList::PostingList(PostingList&& rhs) noexcept {
    _doc_ids = std::move(rhs._doc_ids);
    _positions = std::move(rhs._positions);
}

PostingList& PostingList::operator=(PostingList&& rhs) noexcept {
    _doc_ids = std::move(rhs._doc_ids);
    _positions = std::move(rhs._positions);
    return *this;
}

PostingList::PostingList() {
    _doc_ids.reserve(16);
    _positions.reserve(16);
}

PostingList::~PostingList() = default;

void PostingList::add_posting(rowid_t doc_id, rowid_t pos) {
    if (_doc_ids.empty() || _doc_ids.back() != doc_id) {
        _doc_ids.emplace_back(doc_id);
        _positions.emplace_back(pos);
    } else {
        _positions.back().add(pos);
    }
}

uint32_t PostingList::get_num_doc_ids() const {
    return _doc_ids.size();
}

} // namespace starrocks