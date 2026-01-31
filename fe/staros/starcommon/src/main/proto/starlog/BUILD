package(default_visibility = ["//visibility:public"])

load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_proto_grpc//cpp:defs.bzl", "cc_proto_library")

cc_proto_library(
    name = "streammgr_cc_proto",
    protos = [":streammgr_proto"],
    deps = ["//proto:star_status_cc_proto", ":starlog_common_cc_proto"],
)

cc_proto_library(
    name = "extent_server_service_cc_proto",
    protos = [":extent_server_service_proto"],
    deps = ["//proto:star_status_cc_proto", ":starlog_common_cc_proto"],
)

cc_proto_library(
    name = "starlog_common_cc_proto",
    protos = [
        ":starlog_common_proto",
    ],
)

# defined by *.proto import dependency tree
proto_library(
    name = "starlog_common_proto",
    strip_import_prefix = "/proto",
    srcs = ["common.proto"],
)

proto_library(
    name = "extent_server_service_proto",
    srcs = ["extent_server_service.proto"],
    strip_import_prefix = "/proto",
    deps = ["//proto:star_status_proto", ":starlog_common_proto"],
)

proto_library(
    name = "streammgr_proto",
    srcs = ["streammgr.proto"],
    strip_import_prefix = "/proto",
    deps = ["//proto:star_status_proto", ":starlog_common_proto"],
)
