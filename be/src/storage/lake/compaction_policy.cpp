// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Inc.

#include "storage/lake/compaction_policy.h"

#include "common/config.h"
#include "gutil/strings/join.h"
#include "storage/lake/tablet.h"
#include "storage/tablet_schema.h"

namespace starrocks::lake {

static const double kCompactionScoreThreshold = 0.999;

class BaseAndCumulativeCompactionPolicy : public CompactionPolicy {
public:
    explicit BaseAndCumulativeCompactionPolicy(TabletPtr tablet) : _tablet(std::move(tablet)) {}
    ~BaseAndCumulativeCompactionPolicy() override = default;

    StatusOr<std::vector<RowsetPtr>> pick_rowsets(int64_t version) override;

private:
    double get_cumulative_score();
    double get_base_score();
    StatusOr<std::vector<RowsetPtr>> pick_cumulative_rowsets();
    StatusOr<std::vector<RowsetPtr>> pick_base_rowsets();
    void debug_rowsets(CompactionType type, const std::vector<uint32_t>& input_rowset_ids);

    TabletPtr _tablet;
    TabletMetadataPtr _tablet_metadata;
};

double BaseAndCumulativeCompactionPolicy::get_cumulative_score() {
    if (_tablet_metadata->rowsets_size() == 0) {
        return 0;
    }

    uint32_t segment_num_score = 0;
    size_t rowsets_size = 0;
    for (uint32_t i = _tablet_metadata->cumulative_point(), size = _tablet_metadata->rowsets_size(); i < size; ++i) {
        const auto& rowset = _tablet_metadata->rowsets(i);
        segment_num_score += rowset.overlapped() ? rowset.segments_size() : 1;
        rowsets_size += rowset.data_size();
    }

    double num_score = static_cast<double>(segment_num_score) / config::min_cumulative_compaction_num_singleton_deltas;
    double size_score = static_cast<double>(rowsets_size) / config::min_cumulative_compaction_size;
    double score = std::max(num_score, size_score);
    VLOG(2) << "tablet: " << _tablet->id() << ", cumulative compaction score: " << score
            << ", size_score: " << size_score << ", num_score: " << num_score << ", rowsets_size: " << rowsets_size
            << ", segment_num_score: " << segment_num_score;
    return score;
}

double BaseAndCumulativeCompactionPolicy::get_base_score() {
    uint32_t cumulative_point = _tablet_metadata->cumulative_point();
    if (cumulative_point == 0 || _tablet_metadata->rowsets_size() == 0) {
        return 0;
    }

    uint32_t segment_num_score = 0;
    size_t rowsets_size = 0;
    for (uint32_t i = 1; i < cumulative_point; ++i) {
        const auto& rowset = _tablet_metadata->rowsets(i);
        DCHECK(!rowset.overlapped());
        ++segment_num_score;
        rowsets_size += rowset.data_size();
    }

    double num_score = static_cast<double>(segment_num_score) / config::min_base_compaction_num_singleton_deltas;
    double size_score = static_cast<double>(rowsets_size) / config::min_base_compaction_size;
    double score = std::max(num_score, size_score);
    VLOG(2) << "tablet: " << _tablet->id() << ", base compaction score: " << score << ", size_score: " << size_score
            << ", num_score: " << num_score << ", rowsets_size: " << rowsets_size
            << ", segment_num_score: " << segment_num_score;
    if (score > kCompactionScoreThreshold) {
        return score;
    }

    auto& base_rowset = _tablet_metadata->rowsets(0);
    DCHECK(!base_rowset.overlapped());
    int64_t base_data_size = base_rowset.data_size();
    if (base_data_size > 0) {
        double size_ratio = static_cast<double>(rowsets_size) / base_data_size;
        if (size_ratio >= config::base_cumulative_delta_ratio) {
            score = 1.0;
            VLOG(2) << "satisfy the base compaction size ratio policy. tablet: " << _tablet->id()
                    << ", base disk size: " << base_data_size << ", size_ratio: " << size_ratio;
            return score;
        }
    }
    return score;
}

StatusOr<std::vector<RowsetPtr>> BaseAndCumulativeCompactionPolicy::pick_cumulative_rowsets() {
    std::vector<RowsetPtr> input_rowsets;
    std::vector<uint32_t> input_rowset_ids;
    uint32_t cumulative_point = _tablet_metadata->cumulative_point();
    uint32_t segment_num_score = 0;
    for (uint32_t i = cumulative_point, size = _tablet_metadata->rowsets_size(); i < size; ++i) {
        const auto& rowset = _tablet_metadata->rowsets(i);
        if (rowset.has_delete_predicate()) {
            if (!input_rowsets.empty()) {
                break;
            } else {
                DCHECK(input_rowset_ids.empty());
                DCHECK(segment_num_score == 0);
                continue;
            }
        }

        input_rowset_ids.emplace_back(rowset.id());
        auto metadata_ptr = std::make_shared<RowsetMetadata>(rowset);
        input_rowsets.emplace_back(std::make_shared<Rowset>(_tablet.get(), std::move(metadata_ptr), i));

        segment_num_score += rowset.overlapped() ? rowset.segments_size() : 1;
        if (segment_num_score >= config::max_cumulative_compaction_num_singleton_deltas) {
            break;
        }
    }
    // TODO: need check min_cumulative_compaction_num_singleton_deltas?

    debug_rowsets(CUMULATIVE_COMPACTION, input_rowset_ids);

    return input_rowsets;
}

StatusOr<std::vector<RowsetPtr>> BaseAndCumulativeCompactionPolicy::pick_base_rowsets() {
    std::vector<RowsetPtr> input_rowsets;
    std::vector<uint32_t> input_rowset_ids;
    uint32_t cumulative_point = _tablet_metadata->cumulative_point();
    uint32_t segment_num_score = 0;
    for (uint32_t i = 0; i < cumulative_point; ++i) {
        const auto& rowset = _tablet_metadata->rowsets(i);
        DCHECK(!rowset.overlapped());
        input_rowset_ids.emplace_back(rowset.id());
        auto metadata_ptr = std::make_shared<RowsetMetadata>(rowset);
        input_rowsets.emplace_back(std::make_shared<Rowset>(_tablet.get(), std::move(metadata_ptr), i));

        if (++segment_num_score >= config::max_base_compaction_num_singleton_deltas) {
            break;
        }
    }

    debug_rowsets(BASE_COMPACTION, input_rowset_ids);

    return input_rowsets;
}

void BaseAndCumulativeCompactionPolicy::debug_rowsets(CompactionType type,
                                                      const std::vector<uint32_t>& input_rowset_ids) {
    std::vector<uint32_t> rowset_ids;
    std::vector<uint32_t> delete_rowset_ids;
    for (const auto& rowset : _tablet_metadata->rowsets()) {
        rowset_ids.emplace_back(rowset.id());
        if (rowset.has_delete_predicate()) {
            delete_rowset_ids.emplace_back(rowset.id());
        }
    }
    LOG(INFO) << "pick compaction input rowsets. tablet: " << _tablet->id() << ", type: " << to_string(type)
              << ", version: " << _tablet_metadata->version()
              << ", cumulative point: " << _tablet_metadata->cumulative_point()
              << ", input rowsets size: " << input_rowset_ids.size() << ", input rowsets: ["
              << JoinInts(input_rowset_ids, ",") + "]"
              << ", rowsets: [" << JoinInts(rowset_ids, ",") << "]"
              << ", delete rowsets: [" << JoinInts(delete_rowset_ids, ",") + "]";
}

StatusOr<std::vector<RowsetPtr>> BaseAndCumulativeCompactionPolicy::pick_rowsets(int64_t version) {
    ASSIGN_OR_RETURN(_tablet_metadata, _tablet->get_metadata(version));

    double cumulative_score = get_cumulative_score();
    double base_score = get_base_score();
    if (base_score > cumulative_score) {
        return pick_base_rowsets();
    } else {
        return pick_cumulative_rowsets();
    }
}

StatusOr<CompactionPolicyPtr> CompactionPolicy::create_compaction_policy(TabletPtr tablet) {
    ASSIGN_OR_RETURN(auto tablet_schema, tablet->get_schema());
    if (tablet_schema->keys_type() == PRIMARY_KEYS) {
        return Status::NotSupported("primary key lake tablet compaction policy");
    }
    return std::make_shared<BaseAndCumulativeCompactionPolicy>(std::move(tablet));
}

} // namespace starrocks::lake
