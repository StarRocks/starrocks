#pragma once

#include <array>
#include <optional>
#include <string>
#include <vector>

#include "exec/schema_scanner.h"

namespace starrocks {

class SchemaFeThreadsScanner : public SchemaScanner {
public:
    SchemaFeThreadsScanner();
    ~SchemaFeThreadsScanner() override;

    Status start(RuntimeState* state) override;
    Status get_next(ChunkPtr* chunk, bool* eos) override;

private:
    Status fill_chunk(ChunkPtr* chunk);
    Status _fetch_fe_threads(RuntimeState* state);
    Status _parse_threads_payload(const std::string& fe_id, const std::string& payload);

    static ColumnDesc _s_columns[];
    static constexpr size_t THREAD_FIELD_COUNT = 14;

    struct ThreadRow {
        std::string fe_id;
        std::array<std::optional<std::string>, THREAD_FIELD_COUNT> fields;
    };

    std::vector<ThreadRow> _rows;
    size_t _cur_idx = 0;
};

} // namespace starrocks
