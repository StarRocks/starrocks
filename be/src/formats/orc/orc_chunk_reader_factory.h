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

#pragma once

#include <orc/OrcFile.hh>

#include "common/status.h"
#include "formats/orc/orc_chunk_reader.h"
#include "formats/orc/orc_row_reader_filter.h"

namespace starrocks {

class OrcChunkReaderFactory {
public:
    virtual StatusOr<std::unique_ptr<OrcChunkReader>> create() = 0;

    void set_src_slot_descriptors(std::vector<SlotDescriptor*> src_slot_descriptors) {
        _src_slot_descriptors = src_slot_descriptors;
        remove_nullptr_in_src_slot_descriptors();
    }

    void set_runtime_state(RuntimeState* runtime_state) { _runtime_state = runtime_state; }

    void set_orc_reader(std::unique_ptr<orc::Reader> orc_reader) { _orc_reader = std::move(orc_reader); }

    void set_read_chunk_size(size_t read_chunk_size) { _read_chunk_size = read_chunk_size; }

    void set_case_sensitive(bool case_sensitive) { _case_sensitive = case_sensitive; }

    void set_filename(std::string filename) { _filename = filename; }

    void set_timezone(std::string timezone) { _timezone = timezone; }

    void set_use_orc_column_names(bool use_orc_column_names) { _use_orc_column_names = use_orc_column_names; }

protected:
    void remove_nullptr_in_src_slot_descriptors() {
        // some caller of `OrcChunkReader` may pass nullptr
        // This happens when there are extra fields in broker load specification
        // but those extra fields don't match any fields in native table.
        // For more details, refer to https://github.com/StarRocks/starrocks/issues/1378
        for (auto iter_slot = _src_slot_descriptors.begin(); iter_slot != _src_slot_descriptors.end();
             /*iter_slot++*/) {
            if (*iter_slot == nullptr) {
                iter_slot = _src_slot_descriptors.erase(iter_slot);
            } else {
                iter_slot++;
            }
        }
    }

    std::unique_ptr<orc::Reader> _orc_reader = nullptr;
    std::vector<SlotDescriptor*> _src_slot_descriptors;
    RuntimeState* _runtime_state = nullptr;

    bool _use_orc_column_names = false;
    bool _case_sensitive = false;
    std::string _filename = "";
    std::string _timezone = "";
    size_t _read_chunk_size = 4096;
};

class HdfsOrcReaderFactory : public OrcChunkReaderFactory {
public:
    HdfsOrcReaderFactory(std::vector<SlotDescriptor*> src_slot_descriptors, RuntimeState* runtime_state,
                         std::unique_ptr<orc::Reader> orc_reader, bool use_orc_column_names,
                         std::vector<std::string>* hive_column_names, bool case_sensitive = false,
                         std::string timezone = "", std::string filename = "", size_t read_chunk_size = 4096,
                         OrcChunkReader::LazyLoadContext* lazy_load_ctx = nullptr) {
        // Set default value for hdfs orc reader
        _use_orc_search_arguments = false;

        DCHECK(orc_reader != nullptr);
        if (hive_column_names == nullptr) {
            // If hive_column_names is nullptr, we have to enable use_orc_column_names
            use_orc_column_names = true;
        }

        // Set custom value
        set_src_slot_descriptors(src_slot_descriptors);
        set_runtime_state(runtime_state);
        set_orc_reader(std::move(orc_reader));
        set_use_orc_column_names(use_orc_column_names);
        set_hive_column_names(hive_column_names);
        set_case_sensitive(case_sensitive);
        set_timezone(timezone);
        set_filename(filename);
        set_read_chunk_size(read_chunk_size);
        set_lazy_load_ctx(lazy_load_ctx);
    }

    void set_use_orc_search_arguments(bool use_orc_search_arguments,
                                      std::vector<Expr*> conjuncts = std::vector<Expr*>{},
                                      const RuntimeFilterProbeCollector* runtime_filter_collector = nullptr) {
        _use_orc_search_arguments = use_orc_search_arguments;
        _conjuncts = conjuncts;
        _runtime_filter_collector = runtime_filter_collector;
    }

    void set_use_orc_row_reader_filter(bool use_orc_row_reader_filter, HdfsScannerParams* scanner_params,
                                       HdfsScannerContext* scanner_ctx) {
        if (use_orc_row_reader_filter) {
            DCHECK(scanner_params != nullptr);
            DCHECK(scanner_ctx != nullptr);
        }
        _use_orc_row_reader_filter = use_orc_row_reader_filter;
        _scanner_params = scanner_params;
        _scanner_ctx = scanner_ctx;
    }

    void set_hive_column_names(std::vector<std::string>* hive_column_names) { _hive_column_names = hive_column_names; }

    void set_lazy_load_ctx(OrcChunkReader::LazyLoadContext* lazy_load_ctx) { _lazy_load_ctx = lazy_load_ctx; }

    std::shared_ptr<OrcRowReaderFilter> orc_row_reader_filter() { return _orc_row_reader_filter; }

    StatusOr<std::unique_ptr<OrcChunkReader>> create() {
        std::unique_ptr<OrcChunkReader> reader =
                std::make_unique<OrcChunkReader>(_read_chunk_size, _src_slot_descriptors);
        reader->disable_broker_load_mode();
        reader->set_use_orc_column_names(_use_orc_column_names);
        if (_runtime_state != nullptr) {
            reader->set_runtime_state(_runtime_state);
        }
        reader->set_case_sensitive(_case_sensitive);
        if (!_use_orc_column_names) {
            DCHECK(_hive_column_names != nullptr);
            reader->set_hive_column_names(_hive_column_names);
        }
        if (!_timezone.empty()) {
            RETURN_IF_ERROR(reader->set_timezone(_timezone));
        }
        reader->set_current_file_name(_filename);

        if (config::enable_orc_late_materialization && _lazy_load_ctx != nullptr &&
            _lazy_load_ctx->lazy_load_slots.size() != 0) {
            reader->set_lazy_load_context(_lazy_load_ctx);
        }
        std::unique_ptr<OrcMapping> root_mapping = nullptr;
        ASSIGN_OR_RETURN(root_mapping,
                         OrcMappingFactory::build_mapping(_src_slot_descriptors, _orc_reader->getType(),
                                                          _case_sensitive, _use_orc_column_names, _hive_column_names));
        if (_use_orc_search_arguments) {
            RETURN_IF_ERROR(
                    reader->set_conjuncts_and_runtime_filters(root_mapping, _conjuncts, _runtime_filter_collector));
        }

        if (_use_orc_row_reader_filter) {
            _orc_row_reader_filter =
                    std::make_shared<OrcRowReaderFilter>(*_scanner_params, *_scanner_ctx, reader.get());
            reader->set_row_reader_filter(_orc_row_reader_filter);
        }

        RETURN_IF_ERROR(reader->init(std::move(_orc_reader), std::move(root_mapping)));
        return reader;
    }

private:
    bool _use_orc_search_arguments = true;
    OrcChunkReader::LazyLoadContext* _lazy_load_ctx = nullptr;
    std::vector<Expr*> _conjuncts;
    const RuntimeFilterProbeCollector* _runtime_filter_collector = nullptr;
    std::vector<std::string>* _hive_column_names = nullptr;
    bool _use_orc_row_reader_filter = false;
    HdfsScannerParams* _scanner_params = nullptr;
    HdfsScannerContext* _scanner_ctx = nullptr;
    std::shared_ptr<OrcRowReaderFilter> _orc_row_reader_filter = nullptr;
};

class BrokerLoadOrcReaderFactory : public OrcChunkReaderFactory {
public:
    BrokerLoadOrcReaderFactory(std::vector<SlotDescriptor*> src_slot_descriptors, RuntimeState* runtime_state,
                               std::unique_ptr<orc::Reader> orc_reader, bool strict_mode, size_t read_chunk_size = 4096,
                               std::string timezone = "", std::string filename = "") {
        // Set default value for broker load orc reader
        _use_orc_column_names = true;
        _case_sensitive = true;

        DCHECK(orc_reader != nullptr);

        // Set custom value
        set_src_slot_descriptors(src_slot_descriptors);
        set_runtime_state(runtime_state);
        set_orc_reader(std::move(orc_reader));
        set_strict_mode(strict_mode);
        set_read_chunk_size(read_chunk_size);
        set_timezone(timezone);
        set_filename(filename);
    }

    void set_strict_mode(bool strict_mode) { _strict_mode = strict_mode; }

    StatusOr<std::unique_ptr<OrcChunkReader>> create() {
        std::unique_ptr<OrcChunkReader> reader =
                std::make_unique<OrcChunkReader>(_read_chunk_size, _src_slot_descriptors);
        if (_runtime_state != nullptr) {
            reader->set_runtime_state(_runtime_state);
        }
        reader->set_use_orc_column_names(_use_orc_column_names);
        reader->set_case_sensitive(_case_sensitive);
        reader->set_broker_load_mode(_strict_mode);
        if (!_timezone.empty()) {
            RETURN_IF_ERROR(reader->set_timezone(_timezone));
        }
        reader->drop_nanoseconds_in_datetime();
        reader->set_current_file_name(_filename);

        std::unique_ptr<OrcMapping> root_mapping = nullptr;
        ASSIGN_OR_RETURN(root_mapping,
                         OrcMappingFactory::build_mapping(_src_slot_descriptors, _orc_reader->getType(),
                                                          _case_sensitive, _use_orc_column_names, nullptr));
        RETURN_IF_ERROR(reader->init(std::move(_orc_reader), std::move(root_mapping)));
        return reader;
    }

private:
    bool _strict_mode = false;
};

} // namespace starrocks