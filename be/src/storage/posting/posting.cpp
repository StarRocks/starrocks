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
    if (const auto it = _postings.find(doc_id); it == _postings.end()) {
        _postings.emplace(doc_id, pos);
    } else {
        it->second.add(pos);
    }
}

uint32_t PostingList::get_num_doc_ids() const {
    return _postings.size();
}

Status PostingList::for_each_posting(std::function<Status(rowid_t doc_id, const roaring::Roaring&)>&& func) {
    std::vector<rowid_t> doc_ids;
    doc_ids.reserve(_postings.size());
    for (auto& [doc_id, positions] : _postings) {
        doc_ids.push_back(doc_id);
        positions.flush_pending_adds();
    }

    std::ranges::sort(doc_ids);

    roaring::Roaring single_pos;
    for (const rowid_t doc_id : doc_ids) {
        auto& context = _postings.at(doc_id);
        if (context.is_context()) {
            context.roaring()->runOptimize();
            RETURN_IF_ERROR(func(doc_id, *context.roaring()));
        } else {
            single_pos.clear();
            single_pos.add(context.value());
            RETURN_IF_ERROR(func(doc_id, single_pos));
        }
    }
    return Status::OK();
}

} // namespace starrocks