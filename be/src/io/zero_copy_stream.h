// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <google/protobuf/io/coded_stream.h>
#include <google/protobuf/io/zero_copy_stream.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <google/protobuf/io/zero_copy_stream_impl_lite.h>

namespace starrocks::io {

using CodedInputStream = ::google::protobuf::io::CodedInputStream;
using ZeroCopyInputStream = ::google::protobuf::io::ZeroCopyInputStream;
using ArrayInputStream = ::google::protobuf::io::ArrayInputStream;
using CopyingInputStreamAdaptor = ::google::protobuf::io::CopyingInputStreamAdaptor;
using LimitingInputStream = ::google::protobuf::io::LimitingInputStream;
using FileInputStream = ::google::protobuf::io::FileInputStream;
using IstreamInputStream = ::google::protobuf::io::IstreamInputStream;
using ConcatenatingInputStream = ::google::protobuf::io::ConcatenatingInputStream;

using CodedOutputStream = ::google::protobuf::io::CodedOutputStream;
using ZeroCopyOutputStream = ::google::protobuf::io::ZeroCopyOutputStream;
using ArrayOutputStream = ::google::protobuf::io::ArrayOutputStream;
using StringOutputStream = ::google::protobuf::io::StringOutputStream;
using CopyingOutputStreamAdaptor = ::google::protobuf::io::CopyingOutputStreamAdaptor;
using FileOutputStream = ::google::protobuf::io::FileOutputStream;
using OstreamOutputStream = ::google::protobuf::io::OstreamOutputStream;

} // namespace starrocks::io
