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

void PostingList::finalize() const {
    if (_postings == nullptr || !_postings->is_context()) {
        return;
    }

    _postings->flush_pending_adds();
    _postings->roaring()->runOptimize();
}

Status PostingList::for_each_posting(std::function<Status(rowid_t doc_id, const roaring::Roaring&)>&& func) const {
    if (_postings == nullptr) {
        return Status::OK();
    }

    if (_postings->is_context()) {
        const auto& ref = _postings->roaring()->getRoaringsRef();
        for (const auto& [high, low_bitmap] : ref) {
            RETURN_IF_ERROR(func(high, low_bitmap));
        }
    } else {
        auto val = _postings->value();
        rowid_t high = static_cast<rowid_t>(val >> 32);
        roaring::Roaring single_pos({static_cast<uint32_t>(val)});
        RETURN_IF_ERROR(func(high, single_pos));
    }
    return Status::OK();
}

} // namespace starrocks