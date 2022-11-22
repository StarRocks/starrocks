// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "formats/csv/csv_reader.h"
namespace starrocks::vectorized {

Status CSVReader::next_record(Record* record) {
    if (_limit > 0 && _parsed_bytes > _limit) {
        return Status::EndOfFile("Reached limit");
    }
    char* d;
    size_t pos = 0;
    while ((d = _buff.find(_row_delimiter, pos)) == nullptr) {
        pos = _buff.available();
        _buff.compact();
        if (_buff.free_space() == 0) {
            RETURN_IF_ERROR(_expand_buffer());
        }
        RETURN_IF_ERROR(_fill_buffer());
    }
    size_t l = d - _buff.position();
    *record = Record(_buff.position(), l);
    _buff.skip(l + _row_delimiter_length);
    //               ^^ skip record delimiter.
    _parsed_bytes += l + _row_delimiter_length;
    return Status::OK();
}

Status CSVReader::_expand_buffer() {
    if (UNLIKELY(_storage.size() >= kMaxBufferSize)) {
        return Status::InternalError("CSV line length exceed limit " + std::to_string(kMaxBufferSize));
    }
    size_t new_capacity = std::min(_storage.size() * 2, kMaxBufferSize);
    DCHECK_EQ(_storage.data(), _buff.position()) << "should compact buffer before expand";
    _storage.resize(new_capacity);
    CSVBuffer new_buff(_storage.data(), _storage.size());
    new_buff.add_limit(_buff.available());
    DCHECK_EQ(_storage.data(), new_buff.position());
    DCHECK_EQ(_buff.available(), new_buff.available());
    _buff = new_buff;
    return Status::OK();
}

void CSVReader::split_record(const Record& record, Fields* fields) const {
    const char* value = record.data;
    const char* ptr = record.data;
    const size_t size = record.size;
    const char* end = ptr + size;
    const size_t quoteLen = 1;

    if (_column_separator_length == 1) {
        for (;ptr < end;) {
            // consume all leading spaces
            for (; isspace(*ptr); ptr++, value++);
            // if not started with quote, no quote is expected for this field
            if (*value != '"') {
                // move ptr to the next column separator
                for (; ptr != end && *ptr != _column_separator[0]; ptr++);

                // push the field value
                fields->emplace_back(value, ptr-value);
                // move pointers and continue
                ptr++;
                value = ptr;
                continue;
            } else {
                // Quoted string field

                // move the pointers pass the quote
                ptr += quoteLen;
                value += quoteLen;

                // used to record the starting point of the next field
                const char *nextFieldStartPtr = value;

                for (;;) {
                    // find the next quote
                    int nextQuoteIndex = -1;
                    for (const char *p = ptr; p != end; p++) {
                        if (*p == '"') {
                            nextQuoteIndex = p - ptr;
                            break;
                        }
                    }
                    // find another quote, determine whether it's closing or escaped
                    if (nextQuoteIndex > 0) {
                        const char *ptrAfterNextQuote = ptr + nextQuoteIndex + 1;
                        if (ptrAfterNextQuote == end) {
                            // already the last element, missing closing quote, consider the remaining content except
                            // the quote as the field
                            nextFieldStartPtr = ptrAfterNextQuote + 1;
                            ptr = end;
                            break;
                        } else {
                            if (*ptrAfterNextQuote == '"') {
                                // `""`, so this is an escaped quote. Because Slice is a wrapper for externally
                                // allocated read-only buffer, we are unable to merge two quotes into one without
                                // refactoring in a large scale. Thus, we just leave it as double quotes for now.
                                // TODO: refactor csv reader in a higher level to properly handle escaped quote
                                ptr = ptrAfterNextQuote + 1;
                                continue;
                            } else if (*ptrAfterNextQuote == _column_separator[0]) {
                                // `",` case, field ended
                                nextFieldStartPtr = ptrAfterNextQuote + 1;
                                ptr = value + nextQuoteIndex;
                                break;
                            } else {
                                // invalid `"*` case, but treat `"` as common character and continue to read the
                                // remaining data and load the data with the best effort.
                                ptr = ptrAfterNextQuote + 1;
                                continue;
                            }
                        }
                    } else {
                        // no quote found and hit end of line, everything belong to the column and break out the loop
                        ptr = end;
                        nextFieldStartPtr = end;
                        break;
                    }
                }
                fields->emplace_back(value, ptr - value);
                ptr = nextFieldStartPtr;
                value = nextFieldStartPtr;
            }
        }
    } else {
        const auto* const base = ptr;

        do {
            ptr = static_cast<char*>(
                    memmem(value, size - (value - base), _column_separator.data(), _column_separator_length));
            if (ptr != nullptr) {
                fields->emplace_back(value, ptr - value);
                value = ptr + _column_separator_length;
            }
        } while (ptr != nullptr);

        ptr = record.data + size;
        fields->emplace_back(value, ptr - value);
    }
}

} // namespace starrocks::vectorized
