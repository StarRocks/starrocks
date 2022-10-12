// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/tools/meta_tool.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <gflags/gflags.h>

#include <fstream>
#include <iostream>
#include <set>
#include <string>

#include "common/status.h"
#include "fs/fs.h"
#include "fs/fs_util.h"
#include "gen_cpp/lake_types.pb.h"
#include "gen_cpp/olap_file.pb.h"
#include "gen_cpp/segment.pb.h"
#include "gutil/strings/numbers.h"
#include "gutil/strings/split.h"
#include "gutil/strings/substitute.h"
#include "json2pb/pb_to_json.h"
#include "storage/data_dir.h"
#include "storage/olap_common.h"
#include "storage/olap_define.h"
#include "storage/options.h"
#include "storage/rowset/binary_plain_page.h"
#include "storage/rowset/column_iterator.h"
#include "storage/rowset/column_reader.h"
#include "storage/tablet_meta.h"
#include "storage/tablet_meta_manager.h"
#include "storage/tablet_schema_map.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/path_util.h"

using starrocks::DataDir;
using starrocks::KVStore;
using starrocks::Status;
using starrocks::TabletMeta;
using starrocks::TabletMetaManager;
using starrocks::MetaStoreStats;
using starrocks::Slice;
using starrocks::RandomAccessFile;
using starrocks::MemTracker;
using strings::Substitute;
using starrocks::SegmentFooterPB;
using starrocks::ColumnReader;
using starrocks::BinaryPlainPageDecoder;
using starrocks::PageHandle;
using starrocks::PagePointer;
using starrocks::ColumnIteratorOptions;
using starrocks::PageFooterPB;

const std::string HEADER_PREFIX = "tabletmeta_";

DEFINE_string(root_path, "", "storage root path");
DEFINE_string(operation, "get_meta",
              "valid operation: get_meta, flag, load_meta, delete_meta, delete_rowset_meta, "
              "show_meta, check_table_meta_consistency, print_lake_metadata, print_lake_txn_log");
DEFINE_int64(tablet_id, 0, "tablet_id for tablet meta");
DEFINE_string(tablet_uid, "", "tablet_uid for tablet meta");
DEFINE_int64(table_id, 0, "table id for table meta");
DEFINE_string(rowset_id, "", "rowset_id");
DEFINE_int32(schema_hash, 0, "schema_hash for tablet meta");
DEFINE_string(json_meta_path, "", "absolute json meta file path");
DEFINE_string(pb_meta_path, "", "pb meta file path");
DEFINE_string(tablet_file, "", "file to save a set of tablets");
DEFINE_string(file, "", "segment file path");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the StarRocks BE Meta tool.\n";
    ss << "Stop BE first before use this tool.\n";
    ss << "Usage:\n";
    ss << "./meta_tool --operation=get_meta --root_path=/path/to/storage/path "
          "--tablet_id=tabletid [--schema_hash=schemahash]\n";
    ss << "./meta_tool --operation=load_meta --root_path=/path/to/storage/path "
          "--json_meta_path=path\n";
    ss << "./meta_tool --operation=delete_meta "
          "--root_path=/path/to/storage/path --tablet_id=tabletid "
          "[--schema_hash=schemahash]\n";
    ss << "./meta_tool --operation=delete_meta --tablet_file=file_path\n";
    ss << "./meta_tool --operation=delete_rowset_meta "
          "--root_path=/path/to/storage/path --tablet_uid=tablet_uid "
          "--rowset_id=rowset_id\n";
    ss << "./meta_tool --operation=delete_persistent_index_meta "
          "--root_path=/path/to/storage/path --tablet_id=tabletid\n";
    ss << "./meta_tool --operation=compact_meta --root_path=/path/to/storage/path\n";
    ss << "./meta_tool --operation=get_meta_stats --root_path=/path/to/storage/path\n";
    ss << "./meta_tool --operation=ls --root_path=/path/to/storage/path\n";
    ss << "./meta_tool --operation=show_meta --pb_meta_path=path\n";
    ss << "./meta_tool --operation=show_segment_footer --file=/path/to/segment/file\n";
    ss << "./meta_tool --operation=check_table_meta_consistency --root_path=/path/to/storage/path "
          "--table_id=tableid\n";
    ss << "cat 0001000000001394_0000000000000004.meta | ./meta_tool --operation=print_lake_metadata\n";
    ss << "cat 0001000000001391_0000000000000001.log | ./meta_tool --operation=print_lake_txn_log\n";
    return ss.str();
}

void show_meta() {
    TabletMeta tablet_meta;
    Status s = tablet_meta.create_from_file(FLAGS_pb_meta_path);
    if (!s.ok()) {
        std::cout << "load pb meta file:" << FLAGS_pb_meta_path << " failed"
                  << ", status:" << s << std::endl;
        return;
    }
    std::string json_meta;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    starrocks::TabletMetaPB tablet_meta_pb;
    tablet_meta.to_meta_pb(&tablet_meta_pb);
    json2pb::ProtoMessageToJson(tablet_meta_pb, &json_meta, json_options);
    std::cout << json_meta << std::endl;
}

void get_meta(DataDir* data_dir) {
    std::string value;
    if (FLAGS_schema_hash != 0) {
        auto s = TabletMetaManager::get_json_meta(data_dir, FLAGS_tablet_id, FLAGS_schema_hash, &value);
        if (s.is_not_found()) {
            std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id << ", schema_hash:" << FLAGS_schema_hash
                      << std::endl;
            return;
        } else if (!s.ok()) {
            std::cerr << "fail to get tablet meta: " << s << std::endl;
        }
    } else {
        auto s = TabletMetaManager::get_json_meta(data_dir, FLAGS_tablet_id, &value);
        if (!s.ok()) {
            if (s.is_not_found()) {
                std::cout << "no tablet meta for tablet_id:" << FLAGS_tablet_id << std::endl;
            } else {
                std::cout << "get tablet meta failed: " << s.to_string();
            }
            return;
        }
    }
    std::cout << value << std::endl;
}

void load_meta(DataDir* data_dir) {
    // load json tablet meta into meta
    Status s = TabletMetaManager::load_json_meta(data_dir, FLAGS_json_meta_path);
    if (!s.ok()) {
        std::cout << "load meta failed, status:" << s << std::endl;
        return;
    }
    std::cout << "load meta successfully" << std::endl;
}

void delete_meta(DataDir* data_dir) {
    if (FLAGS_schema_hash != 0) {
        auto st = TabletMetaManager::remove(data_dir, FLAGS_tablet_id, FLAGS_schema_hash);
        if (!st.ok()) {
            std::cout << "delete tablet meta failed for tablet_id:" << FLAGS_tablet_id
                      << ", schema_hash:" << FLAGS_schema_hash << ", status:" << st << std::endl;
            return;
        }
    } else {
        auto st = TabletMetaManager::remove(data_dir, FLAGS_tablet_id);
        if (!st.ok()) {
            std::cout << "delete tablet meta failed for tablet_id:" << FLAGS_tablet_id << " status:" << st.to_string()
                      << std::endl;
            return;
        }
    }
    std::cout << "delete meta successfully" << std::endl;
}

void delete_rowset_meta(DataDir* data_dir) {
    std::string key = "rst_" + FLAGS_tablet_uid + "_" + FLAGS_rowset_id;
    Status s = data_dir->get_meta()->remove(starrocks::META_COLUMN_FAMILY_INDEX, key);
    if (!s.ok()) {
        std::cout << "delete rowset meta failed for tablet_uid:" << FLAGS_tablet_uid
                  << ", rowset_id:" << FLAGS_rowset_id << ", status:" << s << std::endl;
        return;
    }
    std::cout << "delete rowset meta successfully" << std::endl;
}

void delete_persistent_index_meta(DataDir* data_dir) {
    std::string key = "tpi_";
    starrocks::put_fixed64_le(&key, BigEndian::FromHost64(FLAGS_tablet_id));
    Status st = data_dir->get_meta()->remove(starrocks::META_COLUMN_FAMILY_INDEX, key);
    if (st.ok()) {
        std::cout << "delete tablet persistent index meta success, tablet_id: " << FLAGS_tablet_id << std::endl;
    } else {
        std::cout << "delete tablet persistent index meta failed, tablet_id: " << FLAGS_tablet_id
                  << ", status: " << st.to_string() << std::endl;
    }
}

void compact_meta(DataDir* data_dir) {
    uint64_t live_sst_files_size_before = 0;
    uint64_t live_sst_files_size_after = 0;
    if (!data_dir->get_meta()->get_live_sst_files_size(&live_sst_files_size_before)) {
        std::cout << "data dir " << data_dir->path() << " get_live_sst_files_size failed" << std::endl;
    }
    auto s = data_dir->get_meta()->compact();
    if (!s.ok()) {
        std::cout << "data dir " << data_dir->path() << " compact meta failed: " << s << std::endl;
        return;
    }
    if (!data_dir->get_meta()->get_live_sst_files_size(&live_sst_files_size_after)) {
        std::cout << "data dir " << data_dir->path() << " get_live_sst_files_size failed" << std::endl;
    }
    std::cout << "data dir " << data_dir->path() << " compact meta successfully, "
              << "live_sst_files_size_before: " << live_sst_files_size_before
              << " live_sst_files_size_after: " << live_sst_files_size_after << data_dir->get_meta()->get_stats()
              << std::endl;
}

void get_meta_stats(DataDir* data_dir) {
    MetaStoreStats stats;
    auto st = TabletMetaManager::get_stats(data_dir, &stats, false);
    if (!st.ok()) {
        std::cout << "get_meta_stats failed: " << st.to_string() << std::endl;
        return;
    }
    printf("All tablets:\n");
    printf(" tablet: %8zu %10zu\n", stats.tablet_size, stats.tablet_bytes);
    printf("    rst: %8zu %10zu\n", stats.rst_size, stats.rst_bytes);
    printf("Updatable tablets:\n");
    printf(" tablet: %8zu %10zu\n", stats.update_tablet_size, stats.update_tablet_bytes);
    printf("    log: %8zu %10zu\n", stats.log_size, stats.log_bytes);
    printf(" delvec: %8zu %10zu\n", stats.delvec_size, stats.delvec_bytes);
    printf(" rowset: %8zu %10zu\n", stats.rowset_size, stats.rowset_bytes);
    printf("\n  Total: %8zu %10zu\n", stats.total_size, stats.total_bytes);
    printf("Error: %zu\n", stats.error_size);
}

void list_meta(DataDir* data_dir) {
    MetaStoreStats stats;
    auto st = TabletMetaManager::get_stats(data_dir, &stats, true);
    if (!st.ok()) {
        std::cout << "list_meta: " << st.to_string() << std::endl;
        return;
    }
    printf("%8s %8s %10s %4s %10s %6s %10s %6s %10s %18s %24s\n", "table", "tablet", "bytes", "log", "bytes", "delvec",
           "bytes", "rowset", "bytes", "pending_rowset", "pending_rowset_bytes");
    for (auto& e : stats.tablets) {
        auto& st = e.second;
        printf("%8ld %8ld %10zu %4zu %10zu %6zu %10zu %6zu %10lu %18lu %24lu\n", st.table_id, st.tablet_id,
               st.meta_bytes, st.log_size, st.log_bytes, st.delvec_size, st.delvec_bytes, st.rowset_size,
               st.rowset_bytes, st.pending_rowset_size, st.pending_rowset_bytes);
    }
    printf("  Total KV: %zu Bytes: %zu Tablets: %zu Error: %zu\n", stats.total_size, stats.total_bytes,
           stats.tablets.size(), stats.error_size);
}

Status init_data_dir(const std::string& dir, std::unique_ptr<DataDir>* ret, bool read_only = false) {
    std::string root_path;
    Status st = starrocks::fs::canonicalize(dir, &root_path);
    if (!st.ok()) {
        std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string() << std::endl;
        return Status::InternalError("invalid root path");
    }
    starrocks::StorePath path;
    auto res = parse_root_path(root_path, &path);
    if (!res.ok()) {
        std::cout << "parse root path failed:" << root_path << std::endl;
        return res;
    }

    std::unique_ptr<DataDir> p(new (std::nothrow) DataDir(path.path, path.storage_medium));
    if (p == nullptr) {
        std::cout << "new data dir failed" << std::endl;
        return Status::InternalError("new data dir failed");
    }
    st = p->init(read_only);
    if (!st.ok()) {
        std::cout << "data_dir load failed" << std::endl;
        return Status::InternalError("data_dir load failed");
    }

    p.swap(*ret);
    return Status::OK();
}

void batch_delete_meta(const std::string& tablet_file) {
    // each line in tablet file indicate a tablet to delete, format is:
    //      data_dir,tablet_id,schema_hash
    // eg:
    //      /data1/starrocks.HDD,100010,11212389324
    //      /data2/starrocks.HDD,100010,23049230234
    // Or:
    //      data_dir,tablet_id
    // eg:
    //      /data1/starrocks.HDD,100010
    //      /data2/starrocks.HDD,100010
    std::ifstream infile(tablet_file);
    std::string line;
    int err_num = 0;
    int delete_num = 0;
    int total_num = 0;
    std::unordered_map<std::string, std::unique_ptr<DataDir>> dir_map;
    while (std::getline(infile, line)) {
        total_num++;
        std::vector<string> v = strings::Split(line, ",");
        if (!(v.size() == 2 || v.size() == 3)) {
            std::cout << "invalid line in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }
        // 1. get dir
        std::string dir;
        Status st = starrocks::fs::canonicalize(v[0], &dir);
        if (!st.ok()) {
            std::cout << "invalid root dir in tablet_file: " << line << std::endl;
            err_num++;
            continue;
        }

        if (dir_map.find(dir) == dir_map.end()) {
            // new data dir, init it
            std::unique_ptr<DataDir> data_dir_p;
            Status st = init_data_dir(dir, &data_dir_p);
            if (!st.ok()) {
                std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string() << std::endl;
                err_num++;
                continue;
            }
            dir_map[dir] = std::move(data_dir_p);
            std::cout << "get a new data dir: " << dir << std::endl;
        }
        DataDir* data_dir = dir_map[dir].get();
        if (data_dir == nullptr) {
            std::cout << "failed to get data dir: " << line << std::endl;
            err_num++;
            continue;
        }

        // 2. get tablet id/schema_hash
        int64_t tablet_id;
        if (!safe_strto64(v[1].c_str(), &tablet_id)) {
            std::cout << "invalid tablet id: " << line << std::endl;
            err_num++;
            continue;
        }
        if (v.size() == 3) {
            int64_t schema_hash;
            if (!safe_strto64(v[2].c_str(), &schema_hash)) {
                std::cout << "invalid schema hash: " << line << std::endl;
                err_num++;
                continue;
            }

            Status s = TabletMetaManager::remove(data_dir, tablet_id, schema_hash);
            if (!s.ok()) {
                std::cout << "delete tablet meta failed for tablet_id:" << tablet_id << ", schema_hash:" << schema_hash
                          << ", status:" << s << std::endl;
                err_num++;
                continue;
            }
        } else {
            auto s = TabletMetaManager::remove(data_dir, tablet_id);
            if (!s.ok()) {
                std::cout << "delete tablet meta failed for tablet_id:" << tablet_id << ", status:" << s.to_string()
                          << std::endl;
                err_num++;
                continue;
            }
        }

        delete_num++;
    }

    std::cout << "total: " << total_num << ", delete: " << delete_num << ", error: " << err_num << std::endl;
}

Status get_segment_footer(RandomAccessFile* input_file, SegmentFooterPB* footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    const std::string& file_name = input_file->filename();
    ASSIGN_OR_RETURN(const uint64_t file_size, input_file->get_size());

    if (file_size < 12) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < 12", file_name, file_size));
    }

    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(input_file->read_at_fully(file_size - 12, fixed_buf, 12));

    // validate magic number
    const char* k_segment_magic = "D0R1";
    const uint32_t k_segment_magic_length = 4;
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: magic number not match", file_name));
    }

    // read footer PB
    uint32_t footer_length = starrocks::decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption(strings::Substitute("Bad segment file $0: file size $1 < $2", file_name, file_size,
                                                      12 + footer_length));
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(input_file->read_at_fully(file_size - 12 - footer_length, footer_buf.data(), footer_buf.size()));

    // validate footer PB's checksum
    uint32_t expect_checksum = starrocks::decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = starrocks::crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2", file_name,
                                    actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    if (!footer->ParseFromString(footer_buf)) {
        return Status::Corruption(
                strings::Substitute("Bad segment file $0: failed to parse SegmentFooterPB", file_name));
    }
    return Status::OK();
}

void show_segment_footer(const std::string& file_name) {
    auto res = starrocks::FileSystem::Default()->new_random_access_file(file_name);
    if (!res.ok()) {
        std::cout << "open file failed: " << res.status() << std::endl;
        return;
    }
    auto input_file = std::move(res).value();
    SegmentFooterPB footer;
    auto status = get_segment_footer(input_file.get(), &footer);
    if (!status.ok()) {
        std::cout << "get footer failed: " << status.to_string() << std::endl;
        return;
    }
    std::string json_footer;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(footer, &json_footer, json_options);
    if (!ret) {
        std::cout << "Convert PB to json failed" << std::endl;
        return;
    }
    std::cout << json_footer << std::endl;
}

// This function will check the consistency of tablet meta and segment_footer
// #issue 5415
void check_meta_consistency(DataDir* data_dir) {
    std::vector<int64_t> tablet_ids;
    int64_t table_id = FLAGS_table_id;
    auto check_meta_func = [data_dir, &tablet_ids, table_id](int64_t tablet_id, int32_t schema_hash,
                                                             std::string_view value) -> bool {
        starrocks::TabletMetaSharedPtr tablet_meta(new TabletMeta());
        // if deserialize failed, skip it
        if (Status st = tablet_meta->deserialize(value); !st.ok()) {
            return true;
        }
        // tablet is not belong to the table, skip it
        if (tablet_meta->table_id() != table_id) {
            return true;
        }
        std::string tablet_path = data_dir->path() + starrocks::DATA_PREFIX;
        tablet_path = starrocks::path_util::join_path_segments(tablet_path, std::to_string(tablet_meta->shard_id()));
        tablet_path = starrocks::path_util::join_path_segments(tablet_path, std::to_string(tablet_meta->tablet_id()));
        tablet_path = starrocks::path_util::join_path_segments(tablet_path, std::to_string(tablet_meta->schema_hash()));

        auto& tablet_schema = tablet_meta->tablet_schema();
        const std::vector<starrocks::TabletColumn>& columns = tablet_schema.columns();

        for (const auto& rs : tablet_meta->all_rs_metas()) {
            for (int64_t seg_id = 0; seg_id < rs->num_segments(); ++seg_id) {
                std::string seg_path =
                        strings::Substitute("$0/$1_$2.dat", tablet_path, rs->rowset_id().to_string(), seg_id);
                auto res = starrocks::FileSystem::Default()->new_random_access_file(seg_path);
                if (!res.ok()) {
                    continue;
                }
                auto seg_file = std::move(res).value();
                starrocks::SegmentFooterPB footer;
                res = get_segment_footer(seg_file.get(), &footer);
                if (!res.ok()) {
                    continue;
                }

                // unique_id: ordinal: column_type
                std::unordered_map<uint32_t, std::pair<uint32_t, int32_t>> columns_in_footer;
                for (uint32_t ordinal = 0; ordinal < footer.columns().size(); ++ordinal) {
                    const auto& column_pb = footer.columns(ordinal);
                    columns_in_footer.emplace(column_pb.unique_id(), std::make_pair(ordinal, column_pb.type()));
                }
                for (uint32_t col_id = 0; col_id < columns.size(); ++col_id) {
                    uint32_t unique_id = columns[col_id].unique_id();
                    starrocks::FieldType type = columns[col_id].type();
                    auto iter = columns_in_footer.find(unique_id);
                    if (iter == columns_in_footer.end()) {
                        continue;
                    }

                    // find a segment inconsistency, return directly
                    if (iter->second.second != type) {
                        tablet_ids.emplace_back(tablet_id);
                        return true;
                    }

                    // if type is varchar, check length
                    if (type == starrocks::FieldType::OLAP_FIELD_TYPE_VARCHAR) {
                        const auto& column_pb = footer.columns(iter->second.first);
                        if (columns[col_id].length() != column_pb.length()) {
                            tablet_ids.emplace_back(tablet_id);
                            return true;
                        }
                    }
                }
            }
        }
        return true;
    };
    Status load_tablet_status = TabletMetaManager::walk(data_dir->get_meta(), check_meta_func);
    if (tablet_ids.size() > 0) {
        std::cout << "inconsistency tablet:";
    }
    for (size_t i = 0; i < tablet_ids.size(); ++i) {
        std::cout << "," << tablet_ids[i];
    }
    return;
}

int meta_tool_main(int argc, char** argv) {
    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "show_meta") {
        show_meta();
    } else if (FLAGS_operation == "batch_delete_meta") {
        std::string tablet_file;
        Status st = starrocks::fs::canonicalize(FLAGS_tablet_file, &tablet_file);
        if (!st.ok()) {
            std::cout << "invalid tablet file: " << FLAGS_tablet_file << ", error: " << st.to_string() << std::endl;
            return -1;
        }

        batch_delete_meta(tablet_file);
    } else if (FLAGS_operation == "show_segment_footer") {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show dict" << std::endl;
            return -1;
        }
        show_segment_footer(FLAGS_file);
    } else if (FLAGS_operation == "print_lake_metadata") {
        starrocks::lake::TabletMetadataPB metadata;
        if (!metadata.ParseFromIstream(&std::cin)) {
            std::cerr << "Fail to parse tablet metadata\n";
            return -1;
        }
        json2pb::Pb2JsonOptions options;
        options.pretty_json = true;
        std::string json;
        std::string error;
        if (!json2pb::ProtoMessageToJson(metadata, &json, options, &error)) {
            std::cerr << "Fail to convert protobuf to json: " << error << '\n';
            return -1;
        }
        std::cout << json << '\n';
    } else if (FLAGS_operation == "print_lake_txn_log") {
        starrocks::lake::TxnLogPB txn_log;
        if (!txn_log.ParseFromIstream(&std::cin)) {
            std::cerr << "Fail to parse txn log\n";
            return -1;
        }
        json2pb::Pb2JsonOptions options;
        options.pretty_json = true;
        std::string json;
        std::string error;
        if (!json2pb::ProtoMessageToJson(txn_log, &json, options, &error)) {
            std::cerr << "Fail to convert protobuf to json: " << error << '\n';
            return -1;
        }
        std::cout << json << '\n';
    } else {
        // operations that need root path should be written here
        std::set<std::string> valid_operations = {"get_meta",
                                                  "load_meta",
                                                  "delete_meta",
                                                  "delete_rowset_meta",
                                                  "delete_persistent_index_meta",
                                                  "compact_meta",
                                                  "get_meta_stats",
                                                  "ls",
                                                  "check_table_meta_consistency"};
        if (valid_operations.find(FLAGS_operation) == valid_operations.end()) {
            std::cout << "invalid operation:" << FLAGS_operation << std::endl;
            return -1;
        }

        bool read_only = false;
        if (FLAGS_operation == "get_meta" || FLAGS_operation == "get_meta_stats" || FLAGS_operation == "ls" ||
            FLAGS_operation == "check_table_meta_consistency") {
            read_only = true;
        }

        std::unique_ptr<DataDir> data_dir;
        Status st = init_data_dir(FLAGS_root_path, &data_dir, read_only);
        if (!st.ok()) {
            std::cout << "invalid root path:" << FLAGS_root_path << ", error: " << st.to_string() << std::endl;
            return -1;
        }

        if (FLAGS_operation == "get_meta") {
            get_meta(data_dir.get());
        } else if (FLAGS_operation == "load_meta") {
            load_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_meta") {
            delete_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_rowset_meta") {
            delete_rowset_meta(data_dir.get());
        } else if (FLAGS_operation == "delete_persistent_index_meta") {
            delete_persistent_index_meta(data_dir.get());
        } else if (FLAGS_operation == "compact_meta") {
            compact_meta(data_dir.get());
        } else if (FLAGS_operation == "get_meta_stats") {
            get_meta_stats(data_dir.get());
        } else if (FLAGS_operation == "ls") {
            list_meta(data_dir.get());
        } else if (FLAGS_operation == "check_table_meta_consistency") {
            check_meta_consistency(data_dir.get());
        } else {
            std::cout << "invalid operation: " << FLAGS_operation << "\n" << usage << std::endl;
            return -1;
        }
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}
