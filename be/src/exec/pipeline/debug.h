#pragma once

#include "exec/pipeline/pipeline_driver.h"

namespace starrocks::pipeline::debug {
struct DriverInfo {
    int32_t driver_id;
    pipeline::DriverState state;
    std::string driver_desc;
    bool is_fragment_cancelled;
    std::string fragment_status;

    DriverInfo(int32_t driver_id, pipeline::DriverState state, std::string&& driver_desc, bool cancelled,
               std::string status)
            : driver_id(driver_id),
              state(state),
              driver_desc(std::move(driver_desc)),
              is_fragment_cancelled(cancelled),
              fragment_status(std::move(status)) {}
};
using DriverInfoList = std::vector<DriverInfo>;
using FragmentMap = std::unordered_map<TUniqueId, DriverInfoList>;
using QueryMap = std::unordered_map<TUniqueId, FragmentMap>;

struct DriverListAggregator {
    auto operator()(QueryMap& query_map) {
        return [&query_map](pipeline::DriverConstRawPtr driver) {
            TUniqueId query_id = driver->query_ctx()->query_id();
            TUniqueId fragment_id = driver->fragment_ctx()->fragment_instance_id();
            bool is_cancelled = driver->fragment_ctx()->is_canceled();
            std::string status = driver->fragment_ctx()->final_status().to_string();
            int32_t driver_id = driver->driver_id();
            pipeline::DriverState state = driver->driver_state();
            std::string driver_desc = driver->to_readable_string();

            auto fragment_map_it = query_map.find(query_id);
            if (fragment_map_it == query_map.end()) {
                fragment_map_it = query_map.emplace(query_id, FragmentMap()).first;
            }
            auto& fragment_map = fragment_map_it->second;
            auto driver_list_it = fragment_map.find(fragment_id);
            if (driver_list_it == fragment_map.end()) {
                driver_list_it = fragment_map.emplace(fragment_id, DriverInfoList()).first;
            }
            driver_list_it->second.emplace_back(driver_id, state, std::move(driver_desc), is_cancelled, status);
        };
    }
};

struct DumpQueryMapToJson {
    DumpQueryMapToJson(rapidjson::Document::AllocatorType& allocator_) : allocator(allocator_) {}
    rapidjson::Document::AllocatorType& allocator;

    rapidjson::Document operator()(const QueryMap& query_map) {
        rapidjson::Document queries_obj;
        queries_obj.SetArray();
        for (const auto& [query_id, fragment_map] : query_map) {
            rapidjson::Document fragments_obj;
            fragments_obj.SetArray();
            for (const auto& [fragment_id, driver_info_list] : fragment_map) {
                rapidjson::Document drivers_obj;
                drivers_obj.SetArray();
                bool is_fragment_cancelled = false;
                std::string status;
                for (const auto& driver_info : driver_info_list) {
                    rapidjson::Document driver_obj;
                    driver_obj.SetObject();

                    driver_obj.AddMember("driver_id", rapidjson::Value(driver_info.driver_id), allocator);
                    driver_obj.AddMember("state", rapidjson::Value(ds_to_string(driver_info.state).c_str(), allocator),
                                         allocator);
                    driver_obj.AddMember("driver_desc", rapidjson::Value(driver_info.driver_desc.c_str(), allocator),
                                         allocator);

                    drivers_obj.PushBack(driver_obj, allocator);
                    if (driver_info.is_fragment_cancelled) {
                        is_fragment_cancelled = true;
                    }
                    status = std::move(driver_info.fragment_status);
                }

                rapidjson::Document fragment_obj;
                fragment_obj.SetObject();
                fragment_obj.AddMember("fragment_id", rapidjson::Value(print_id(fragment_id).c_str(), allocator),
                                       allocator);
                fragment_obj.AddMember(
                        "fragment_status",
                        rapidjson::Value((status + (is_fragment_cancelled ? ", cancelled" : "")).c_str(), allocator),
                        allocator);
                fragment_obj.AddMember("drivers", drivers_obj, allocator);
                fragments_obj.PushBack(fragment_obj, allocator);
            }

            rapidjson::Document query_obj;
            query_obj.SetObject();
            query_obj.AddMember("query_id", rapidjson::Value(print_id(query_id).c_str(), allocator), allocator);
            query_obj.AddMember("fragments", fragments_obj, allocator);
            queries_obj.PushBack(query_obj, allocator);
        }

        return queries_obj;
    }
};

} // namespace starrocks::pipeline::debug