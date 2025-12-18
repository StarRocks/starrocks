#include "posting.h"

#include "storage/posting/encoder.h"

namespace starrocks {

PostingList::PostingList(PostingList&& rhs) noexcept {
    _postings = std::move(rhs._postings);
}

PostingList& PostingList::operator=(PostingList&& rhs) noexcept {
    _postings = std::move(rhs._postings);
    return *this;
}

PostingList::PostingList() = default;

PostingList::~PostingList() = default;

void PostingList::add_posting(rowid_t doc_id, rowid_t pos) {
    const uint64_t val = static_cast<uint64_t>(doc_id) << 32 | static_cast<uint64_t>(pos);
    if (_postings == nullptr) {
        _postings = std::make_unique<BitmapUpdateContextRefOrSingleValue<uint64_t>>(val);
    } else {
        _postings->add(val);
    }
}

roaring::Roaring PostingList::get_all_doc_ids() const {
    const auto postings = _internal_get_all_postings();
    return postings.getAllHighBits();
}

roaring::Roaring PostingList::get_positions(rowid_t doc_id) const {
    const auto postings = _internal_get_all_postings();
    return postings.getLowBitsRoaring(doc_id);
}

detail::Roaring64Map PostingList::_internal_get_all_postings() const {
    if (_postings == nullptr) {
        return {};
    }

    _postings->flush_pending_adds();

    detail::Roaring64Map roaring;
    if (_postings->is_context()) {
        roaring.add(_postings->value());
    } else {
        roaring = *(_postings->roaring());
    }
    return roaring;
}

} // namespace starrocks