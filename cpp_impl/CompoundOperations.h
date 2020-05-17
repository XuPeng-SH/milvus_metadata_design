#pragma once

#include "ResourceOperations.h"

class BuildOperation : public Operations {
public:
    static constexpr const char* Name = "Build";
    using BaseT = Operations;

    BuildOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    BuildOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    bool DoExecute() override;
    bool PreExecute() override;

    SegmentFilePtr NewSegmentFile(const SegmentFileContext& context) {
        SegmentFileOperation new_sf_op(context, prev_ss_);
        new_sf_op.OnExecute();
        context_.new_segment_files.push_back(new_sf_op.GetResource());
        return new_sf_op.GetResource();
    }
};

class NewSegmentOperation : public Operations {
public:
    using BaseT = Operations;

    NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    NewSegmentOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    bool DoExecute() override {
        auto i = 0;
        for(; i<context_.new_segment_files.size(); ++i) {
            std::any_cast<SegmentFilePtr>(steps_[i])->Activate();
        }
        std::any_cast<SegmentCommitPtr>(steps_[i++])->Activate();
        std::any_cast<PartitionCommitPtr>(steps_[i++])->Activate();
        std::any_cast<CollectionCommitPtr>(steps_[i++])->Activate();
        return true;
    }

    bool PreExecute() override {
        // PXU TODO:
        // 1. Check all requried field elements have related segment files
        // 2. Check Stale and others
        SegmentCommitOperation op(context_, prev_ss_);
        op.OnExecute();
        context_.new_segment_commit = op.GetResource();
        if (!context_.new_segment_commit) return false;

        PartitionCommitOperation pc_op(context_, prev_ss_);
        pc_op.OnExecute();

        OperationContext cc_context;
        cc_context.new_partition_commit = pc_op.GetResource();
        CollectionCommitOperation cc_op(cc_context, prev_ss_);
        cc_op.OnExecute();

        for (auto& new_segment_file : context_.new_segment_files) {
            AddStep(*new_segment_file);
        }
        AddStep(*context_.new_segment_commit);
        AddStep(*pc_op.GetResource());
        AddStep(*cc_op.GetResource());
        return true;
    }

    SegmentPtr NewSegment() {
        SegmentOperation op(context_, prev_ss_);
        op.OnExecute();
        context_.new_segment = op.GetResource();
        return context_.new_segment;
    }

    SegmentFilePtr NewSegmentFile(const SegmentFileContext& context) {
        auto c = context;
        c.segment_id = context_.new_segment->GetID();
        c.partition_id = context_.new_segment->GetPartitionId();
        SegmentFileOperation new_sf_op(c, prev_ss_);
        new_sf_op.OnExecute();
        context_.new_segment_files.push_back(new_sf_op.GetResource());
        return new_sf_op.GetResource();
    }

};
