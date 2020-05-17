#pragma once
#include "Operations.h"

class CollectionCommitOperation : public CommitOperation<CollectionCommit> {
public:
    using BaseT = CommitOperation<CollectionCommit>;
    CollectionCommitOperation(OperationContext context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    CollectionCommitOperation(OperationContext context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    CollectionCommitPtr GetPrevResource() const override {
        return prev_ss_->GetCollectionCommit();
    }

    bool DoExecute() override {
        auto prev_resource = GetPrevResource();
        if (!prev_resource) return false;
        resource_ = std::make_shared<CollectionCommit>(*prev_resource);
        if (context_.new_partition_commit) {
            auto prev_partition_commit = prev_ss_->GetPartitionCommit(
                    context_.new_partition_commit->GetPartitionId());
            resource_->GetMappings().erase(prev_partition_commit->GetID());
            resource_->GetMappings().insert(context_.new_partition_commit->GetID());
        } else if (context_.new_schema_commit) {
            resource_->SetSchemaId(context_.new_schema_commit->GetID());
        }
        resource_->SetID(0);
        AddStep(*BaseT::resource_);
        return true;
    }
};

class PartitionCommitOperation : public CommitOperation<PartitionCommit> {
public:
    using BaseT = CommitOperation<PartitionCommit>;
    PartitionCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    PartitionCommitOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    PartitionCommitPtr GetPrevResource() const override {
        auto& segment_commit = context_.new_segment_commit;
        return prev_ss_->GetPartitionCommit(segment_commit->GetPartitionId());
    }

    bool DoExecute() override {
        auto prev_resource = GetPrevResource();
        if (prev_resource) {
            resource_ = std::make_shared<PartitionCommit>(*prev_resource);
            resource_->SetID(0);
            auto prev_segment_commit = prev_ss_->GetSegmentCommit(
                    context_.new_segment_commit->GetSegmentId());
            if (prev_segment_commit)
                resource_->GetMappings().erase(prev_segment_commit->GetID());
        } else {
            resource_ = std::make_shared<PartitionCommit>(prev_ss_->GetCollectionId(),
                                                          context_.new_segment_commit->GetPartitionId());
        }

        resource_->GetMappings().insert(context_.new_segment_commit->GetID());
        AddStep(*resource_);
        return true;
    }
};

class SegmentCommitOperation : public CommitOperation<SegmentCommit> {
public:
    using BaseT = CommitOperation<SegmentCommit>;
    SegmentCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    SegmentCommitOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    SegmentCommit::Ptr GetPrevResource() const override {
        if (context_.new_segment_files.size() > 0) {
            return prev_ss_->GetSegmentCommit(context_.new_segment_files[0]->GetSegmentId());
        }
        return nullptr;
    }

    bool DoExecute() override {
        auto prev_resource = GetPrevResource();

        if (prev_resource) {
            resource_ = std::make_shared<SegmentCommit>(*prev_resource);
            resource_->SetID(0);
            if (context_.stale_segment_file) {
                resource_->GetMappings().erase(context_.stale_segment_file->GetID());
            }
        } else {
            resource_ = std::make_shared<SegmentCommit>(prev_ss_->GetLatestSchemaCommitId(),
                                                        context_.new_segment_files[0]->GetPartitionId(),
                                                        context_.new_segment_files[0]->GetSegmentId());
        }
        for(auto& new_segment_file : context_.new_segment_files) {
            resource_->GetMappings().insert(new_segment_file->GetID());
        }
        AddStep(*resource_);
        return true;
    }
};

class SegmentOperation : public CommitOperation<Segment> {
public:
    using BaseT = CommitOperation<Segment>;
    SegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    SegmentOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    bool DoExecute() override {
        if (!context_.prev_partition) {
            return false;
        }
        resource_ = std::make_shared<Segment>(context_.prev_partition->GetID());
        AddStep(*resource_);
        return true;
    }
};

class SegmentFileOperation : public CommitOperation<SegmentFile> {
public:
    using BaseT = CommitOperation<SegmentFile>;
    SegmentFileOperation(const SegmentFileContext& sc, ScopedSnapshotT prev_ss)
        : BaseT(OperationContext(), prev_ss), context_(sc) {};
    SegmentFileOperation(const SegmentFileContext& sc, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(OperationContext(), collection_id, commit_id), context_(sc) {};

    bool DoExecute() override;

protected:
    SegmentFileContext context_;
};

bool
SegmentFileOperation::DoExecute() {
    auto field_element_id = prev_ss_->GetFieldElementId(context_.field_name, context_.field_element_name);
    resource_ = std::make_shared<SegmentFile>(context_.partition_id, context_.segment_id, field_element_id);
    AddStep(*resource_);
    return true;
}
