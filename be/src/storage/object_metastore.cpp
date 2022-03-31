// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#include "storage/object_metastore.h"

#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>

#include "env/env_s3.h"
#include "gutil/strings/util.h"
#include "storage/tablet_meta.h"

namespace starrocks {

static const char* kTabletMeta = "tabletmeta_";
static const char* kRowsetMetaPrefix = "rst_";

// Metastore based on AWS S3.
class ObjectMetastore : public Metastore {
public:
    explicit ObjectMetastore(const std::string& meta_dir) : _meta_dir(meta_dir) {}
    ~ObjectMetastore() override = default;

    ObjectMetastore(const ObjectMetastore&) = delete;
    void operator=(const ObjectMetastore&) = delete;
    ObjectMetastore(ObjectMetastore&&) = delete;
    void operator=(ObjectMetastore&&) = delete;

    // put object
    Status add_tablet_meta(const TabletMeta& tablet_meta) override;
    Status update_tablet_meta(const TabletMeta& tablet_meta) override;
    // get object
    StatusOr<TabletMetaSharedPtr> get_tablet_meta(TTabletId tablet_id, TSchemaHash schema_hash) override;
    // delete object
    Status remove_tablet_meta(TTabletId tablet_id, TSchemaHash schema_hash) override;

    // put object
    Status add_rowset_meta(const RowsetMeta& rowset_meta) override;
    Status update_rowset_meta(const RowsetMeta& rowset_meta) override;
    // get object
    StatusOr<RowsetMetaSharedPtr> get_rowset_meta(const TabletUid& tablet_uid, const RowsetId& rowset_id) override;
    // list objects and get object
    StatusOr<std::vector<RowsetMetaSharedPtr>> get_rowset_metas(const TabletUid& tablet_uid) override;
    // delete object
    Status remove_rowset_meta(const TabletUid& tablet_uid, const RowsetId& rowset_id) override;

private:
    StatusOr<RowsetMetaSharedPtr> get_rowset_meta(const std::string& path);

    Status put_object(const std::string& path, const std::string& object);
    StatusOr<std::string> get_object(const std::string& path);
    StatusOr<std::vector<std::string>> list_objects_by_prefix(const std::string& prefix_path);
    Status delete_object(const std::string& path);

    std::string _meta_dir;
};

Status ObjectMetastore::add_tablet_meta(const TabletMeta& tablet_meta) {
    auto path = fmt::format("{}/{}{}", _meta_dir, kTabletMeta, tablet_meta.tablet_id());
    TabletMetaPB tablet_meta_pb;
    tablet_meta.to_meta_pb(&tablet_meta_pb);
    auto object = tablet_meta_pb.SerializeAsString();
    return put_object(path, object);
}

Status ObjectMetastore::update_tablet_meta(const TabletMeta& tablet_meta) {
    return add_tablet_meta(tablet_meta);
}

StatusOr<TabletMetaSharedPtr> ObjectMetastore::get_tablet_meta(TTabletId tablet_id, TSchemaHash schema_hash) {
    auto path = fmt::format("{}/{}{}", _meta_dir, kTabletMeta, tablet_id);
    ASSIGN_OR_RETURN(auto meta_binary, get_object(path));
    auto tablet_meta = std::make_shared<TabletMeta>();
    RETURN_IF_ERROR(tablet_meta->deserialize(meta_binary));
    return tablet_meta;
}

Status ObjectMetastore::remove_tablet_meta(TTabletId tablet_id, TSchemaHash schema_hash) {
    auto path = fmt::format("{}/{}{}", _meta_dir, kTabletMeta, tablet_id);
    return delete_object(path);
}

Status ObjectMetastore::add_rowset_meta(const RowsetMeta& rowset_meta) {
    auto path = fmt::format("{}/{}{}_{}", _meta_dir, kRowsetMetaPrefix, rowset_meta.tablet_uid().to_string(),
                            rowset_meta.rowset_id().to_string());
    RowsetMetaPB rowset_meta_pb;
    rowset_meta.to_rowset_pb(&rowset_meta_pb);
    auto object = rowset_meta_pb.SerializeAsString();
    return put_object(path, object);
}

Status ObjectMetastore::update_rowset_meta(const RowsetMeta& rowset_meta) {
    return add_rowset_meta(rowset_meta);
}

StatusOr<RowsetMetaSharedPtr> ObjectMetastore::get_rowset_meta(const TabletUid& tablet_uid, const RowsetId& rowset_id) {
    auto path = fmt::format("{}/{}{}_{}", _meta_dir, kRowsetMetaPrefix, tablet_uid.to_string(), rowset_id.to_string());
    return get_rowset_meta(path);
}

StatusOr<RowsetMetaSharedPtr> ObjectMetastore::get_rowset_meta(const std::string& path) {
    ASSIGN_OR_RETURN(auto meta_binary, get_object(path));
    RowsetMetaPB rowset_meta_pb;
    if (!rowset_meta_pb.ParseFromString(meta_binary)) {
        return Status::InternalError("failed to parse RowsetMetaPB from string");
    }
    auto rowset_meta = std::make_shared<RowsetMeta>();
    if (!rowset_meta->init_from_pb(rowset_meta_pb)) {
        return Status::InternalError("failed to init RowsetMeta from pb");
    }
    return rowset_meta;
}

StatusOr<std::vector<RowsetMetaSharedPtr>> ObjectMetastore::get_rowset_metas(const TabletUid& tablet_uid) {
    auto object_prefix = fmt::format("{}/{}{}_", _meta_dir, kRowsetMetaPrefix, tablet_uid.to_string());
    ASSIGN_OR_RETURN(auto objects, list_objects_by_prefix(object_prefix));
    std::vector<RowsetMetaSharedPtr> rowset_metas;
    rowset_metas.reserve(objects.size());
    for (const auto& object : objects) {
        auto path = fmt::format("{}/{}", _meta_dir, object);
        auto rowset_meta_st = get_rowset_meta(path);
        if (!rowset_meta_st.ok()) {
            return rowset_meta_st.status();
        }
        rowset_metas.emplace_back(rowset_meta_st.value());
    }
    return std::move(rowset_metas);
}

Status ObjectMetastore::remove_rowset_meta(const TabletUid& tablet_uid, const RowsetId& rowset_id) {
    auto path = fmt::format("{}/{}{}_{}", _meta_dir, kRowsetMetaPrefix, tablet_uid.to_string(), rowset_id.to_string());
    return delete_object(path);
}

Status ObjectMetastore::put_object(const std::string& path, const std::string& object) {
    ASSIGN_OR_RETURN(auto env, Env::CreateUniqueFromString(path));
    ASSIGN_OR_RETURN(auto file, env->new_writable_file(path));
    RETURN_IF_ERROR(file->append(Slice(object)));
    RETURN_IF_ERROR(file->sync());
    return file->close();
}

StatusOr<std::string> ObjectMetastore::get_object(const std::string& path) {
    ASSIGN_OR_RETURN(auto env, Env::CreateUniqueFromString(path));
    ASSIGN_OR_RETURN(auto file, env->new_random_access_file(path));
    ASSIGN_OR_RETURN(auto size, file->get_size());
    std::vector<char> content;
    raw::stl_vector_resize_uninitialized(&content, size);
    RETURN_IF_ERROR(file->read_at_fully(0, content.data(), size));
    return std::string(content.data(), content.size());
}

StatusOr<std::vector<std::string>> ObjectMetastore::list_objects_by_prefix(const std::string& prefix_path) {
    S3URI uri;
    if (!uri.parse(prefix_path)) {
        return Status::InvalidArgument(fmt::format("Invalid S3 URI {}", prefix_path));
    }

    std::vector<std::string> objects;
    auto client = new_s3client(uri);
    Aws::S3::Model::ListObjectsV2Request request;
    Aws::S3::Model::ListObjectsV2Result result;
    request.WithBucket(uri.bucket()).WithPrefix(uri.key()).WithDelimiter("/");
    do {
        auto outcome = client->ListObjectsV2(request);
        if (!outcome.IsSuccess()) {
            return Status::IOError(
                    fmt::format("S3: fail to list {}: {}", prefix_path, outcome.GetError().GetMessage()));
        }
        result = outcome.GetResultWithOwnership();
        request.SetContinuationToken(result.GetNextContinuationToken());
        // Get file objects except directory.
        for (auto&& obj : result.GetContents()) {
            DCHECK(HasPrefixString(obj.GetKey(), uri.key()));
            std::string_view key = obj.GetKey();
            size_t pos = key.find_last_of("/");
            if (pos == std::string::npos) {
                objects.emplace_back(key);
            } else {
                objects.emplace_back(key.substr(pos + 1));
            }
        }
    } while (result.GetIsTruncated());

    return std::move(objects);
}

Status ObjectMetastore::delete_object(const std::string& path) {
    ASSIGN_OR_RETURN(auto env, Env::CreateUniqueFromString(path));
    return env->delete_file(path);
}

std::unique_ptr<Metastore> new_object_metastore(const std::string& meta_dir) {
    return std::make_unique<ObjectMetastore>(meta_dir);
}

} // namespace starrocks