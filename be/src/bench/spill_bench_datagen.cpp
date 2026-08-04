// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Spill bench data generator v2: materializes the FROZEN benchmark dataset.
//
// Each dataset (one leaf of the codec decision tree, see spill_bench_data.h) is written as
// a framed, self-validating file so the pipeline bench loads the exact same bytes across
// code versions -- the before/after contract for spill optimizations.
//
// File format (<dir>/<name>.spb, all u32 little-endian):
//   header: magic 'SPB1' | version=1 | chunk_count | rows_per_chunk | frames_crc32c | reserved
//   frames: [u32 payload_size | payload]...   payload = ColumnArraySerde level-0 concat
// frames_crc32c = crc32c over all frame bytes (sizes + payloads); readers must verify it.
//
// Usage:
//   spill_bench_datagen [--dir=<out dir>] [--rows=4096]
//                       [--bytes-per-dataset=268435456] [--seed=<u64>]
// --dir defaults to <system temp>/spill_bench_datasets, which is also where
// spill_pipeline_bench looks unless SPILL_BENCH_DATA_DIR overrides it.
//
// Default: 20 datasets x 256MB ~= 5GB total (well under the 10G budget).

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <string>
#include <vector>

#include "base/hash/crc32c.h"
#include "bench/spill_bench_data.h"
#include "column/chunk.h"
#include "column/column.h"
#include "column/serde/column_array_serde.h"

namespace starrocks::spill_bench {

using serde::ColumnArraySerde;

static constexpr uint32_t kMagic = 0x31425053; // "SPB1" little-endian
static constexpr uint32_t kVersion = 1;
static constexpr size_t kHeaderSize = 6 * sizeof(uint32_t);

static const std::vector<int> kManifestLevels = {0, 2, 4, 7};

static size_t serialize_chunk_level(const Chunk& chunk, int level, std::string& scratch) {
    size_t max_payload = 0;
    for (const auto& c : chunk.columns()) max_payload += ColumnArraySerde::max_serialized_size(*c, level);
    scratch.resize(max_payload > 0 ? max_payload : 1);
    auto* base = reinterpret_cast<uint8_t*>(scratch.data());
    uint8_t* buf = base;
    for (const auto& c : chunk.columns()) {
        auto r = ColumnArraySerde::serialize(*c, buf, false, level);
        if (!r.ok()) {
            // A datagen that silently truncated a frame here would poison every future bench run
            // (the frozen dataset's crc would still validate, masking the missing bytes). Abort.
            fprintf(stderr, "datagen serialize failed: %s\n", std::string(r.status().message()).c_str());
            abort();
        }
        buf = r.value();
    }
    return static_cast<size_t>(buf - base);
}

static size_t chunk_raw_bytes(const Chunk& chunk) {
    size_t total = 0;
    for (const auto& c : chunk.columns()) total += c->byte_size();
    return total;
}

static std::string default_out_dir() {
    return (std::filesystem::temp_directory_path() / "spill_bench_datasets").string();
}

struct Args {
    std::string dir = default_out_dir();
    size_t rows = 4096;
    size_t bytes_per_dataset = 256ull * 1024 * 1024;
    uint64_t seed = 0;
    bool has_seed = false;
};

static Args parse_args(int argc, char** argv) {
    Args a;
    for (int i = 1; i < argc; ++i) {
        std::string s = argv[i];
        auto eat = [&](const char* key, auto&& fn) -> bool {
            std::string prefix = std::string(key) + "=";
            if (s.rfind(prefix, 0) == 0) {
                fn(s.substr(prefix.size()));
                return true;
            }
            return false;
        };
        if (eat("--dir", [&](const std::string& v) { a.dir = v; })) continue;
        if (eat("--rows", [&](const std::string& v) { a.rows = std::stoull(v); })) continue;
        if (eat("--bytes-per-dataset", [&](const std::string& v) { a.bytes_per_dataset = std::stoull(v); })) continue;
        if (eat("--seed", [&](const std::string& v) {
                a.seed = std::stoull(v);
                a.has_seed = true;
            }))
            continue;
        fprintf(stderr, "unknown arg: %s\n", s.c_str());
    }
    return a;
}

static void put_u32(std::string& s, uint32_t v) {
    s.append(reinterpret_cast<const char*>(&v), sizeof(v));
}

static int run(int argc, char** argv) {
    Args args = parse_args(argc, argv);
    namespace fs = std::filesystem;
    fs::path root(args.dir);
    std::error_code ec;
    fs::create_directories(root, ec);
    if (ec) {
        fprintf(stderr, "cannot create %s: %s\n", root.c_str(), ec.message().c_str());
        return 1;
    }

    GenConfig cfg;
    cfg.num_rows = args.rows;
    if (args.has_seed) cfg.seed = args.seed;

    std::string manifest;
    manifest += "{\n";
    manifest += "  \"version\": 1,\n";
    manifest += "  \"config\": {\"rows_per_chunk\": " + std::to_string(cfg.num_rows) +
                ", \"bytes_per_dataset\": " + std::to_string(args.bytes_per_dataset) +
                ", \"seed\": " + std::to_string(cfg.seed) + "},\n";
    manifest += "  \"datasets\": [\n";

    std::string scratch;
    size_t total_on_disk = 0;
    const auto& all = all_datasets();
    for (size_t di = 0; di < all.size(); ++di) {
        const auto& info = all[di];
        fs::path out_path = root / (std::string(info.name) + ".spb");
        std::ofstream out(out_path, std::ios::binary | std::ios::trunc);

        // placeholder header, patched after the frames are written
        std::string header(kHeaderSize, '\0');
        out.write(header.data(), header.size());

        uint32_t chunk_count = 0;
        uint32_t frames_crc = 0;
        size_t raw_bytes_total = 0;
        size_t frame_bytes = 0;
        ChunkPtr sample;
        while (frame_bytes < args.bytes_per_dataset) {
            GenConfig chunk_cfg = cfg;
            chunk_cfg.seed = cfg.seed + chunk_count; // per-chunk stream, deterministic
            ChunkPtr chunk = build_chunk(info.id, chunk_cfg);
            if (sample == nullptr) sample = chunk;
            raw_bytes_total += chunk_raw_bytes(*chunk);
            size_t n = serialize_chunk_level(*chunk, 0, scratch);
            uint32_t n32 = static_cast<uint32_t>(n);
            frames_crc = crc32c::Extend(frames_crc, reinterpret_cast<const char*>(&n32), sizeof(n32));
            frames_crc = crc32c::Extend(frames_crc, scratch.data(), n);
            out.write(reinterpret_cast<const char*>(&n32), sizeof(n32));
            out.write(scratch.data(), n);
            frame_bytes += sizeof(n32) + n;
            ++chunk_count;
        }
        // patch the real header
        header.clear();
        put_u32(header, kMagic);
        put_u32(header, kVersion);
        put_u32(header, chunk_count);
        put_u32(header, static_cast<uint32_t>(cfg.num_rows));
        put_u32(header, frames_crc);
        put_u32(header, 0);
        out.seekp(0);
        out.write(header.data(), header.size());
        out.close();
        size_t file_bytes = kHeaderSize + frame_bytes;
        total_on_disk += file_bytes;

        // per-level sizing of the first chunk, as a compression-ratio reference
        size_t sample_raw = chunk_raw_bytes(*sample);
        std::string level_json;
        for (size_t li = 0; li < kManifestLevels.size(); ++li) {
            int level = kManifestLevels[li];
            size_t enc = serialize_chunk_level(*sample, level, scratch);
            double ratio = sample_raw > 0 ? static_cast<double>(enc) / sample_raw : 0.0;
            level_json += "{\"level\": " + std::to_string(level) + ", \"enc_bytes\": " + std::to_string(enc) +
                          ", \"ratio\": " + std::to_string(ratio) + "}";
            if (li + 1 < kManifestLevels.size()) level_json += ", ";
        }

        char crc_hex[16];
        snprintf(crc_hex, sizeof(crc_hex), "%08x", frames_crc);
        manifest += "    {\"name\": \"" + std::string(info.name) + "\", ";
        manifest += "\"tree_leaf\": \"" + std::string(info.redundancy) + "\", ";
        manifest += "\"num_columns\": " + std::to_string(sample->num_columns()) + ", ";
        manifest += "\"chunks\": " + std::to_string(chunk_count) + ", ";
        manifest += "\"raw_bytes\": " + std::to_string(raw_bytes_total) + ", ";
        manifest += "\"file_bytes\": " + std::to_string(file_bytes) + ", ";
        manifest += "\"frames_crc32c\": \"" + std::string(crc_hex) + "\", ";
        manifest += "\"levels\": [" + level_json + "]}";
        if (di + 1 < all.size()) manifest += ",";
        manifest += "\n";

        printf("[datagen] %-16s leaf=%-14s cols=%zu chunks=%u raw=%.1fMB file=%.1fMB crc=%s\n", info.name,
               info.redundancy, sample->num_columns(), chunk_count, raw_bytes_total / 1048576.0, file_bytes / 1048576.0,
               crc_hex);
        fflush(stdout);
    }
    manifest += "  ],\n";
    manifest += "  \"total_on_disk_bytes\": " + std::to_string(total_on_disk) + "\n";
    manifest += "}\n";

    fs::path manifest_path = root / "manifest.json";
    std::ofstream mf(manifest_path, std::ios::trunc);
    mf << manifest;
    mf.close();

    printf("[datagen] wrote %s, total on-disk %.2f GB\n", manifest_path.c_str(),
           total_on_disk / (1024.0 * 1024.0 * 1024.0));
    return 0;
}

} // namespace starrocks::spill_bench

int main(int argc, char** argv) {
    return starrocks::spill_bench::run(argc, argv);
}
