#pragma once

#include "Snapshots.h"
#include "Store.h"
#include "Context.h"
#include <assert.h>
#include <vector>
#include <any>

using StepsT = std::vector<std::any>;

enum OpStatus {
    OP_PENDING = 0,
    OP_OK,
    OP_STALE_OK,
    OP_STALE_CANCEL,
    OP_STALE_RESCHEDULE,
    OP_FAIL_INVALID_PARAMS,
    OP_FAIL_DUPLICATED,
    OP_FAIL_FLUSH_META
};

class Operations {
public:
    /* static constexpr const char* Name = Derived::Name; */
    Operations(const OperationContext& context, ScopedSnapshotT prev_ss);
    Operations(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    const ScopedSnapshotT& GetPrevSnapshot() const {return prev_ss_;}

    virtual bool IsStale() const;

    template<typename StepT>
    void AddStep(const StepT& step);
    void SetStepResult(ID_TYPE id) { ids_.push_back(id); }

    StepsT& GetSteps() { return steps_; }

    virtual void OnExecute();
    virtual bool PreExecute();
    virtual bool DoExecute();
    virtual bool PostExecute();

    virtual ~Operations() {}

protected:
    OperationContext context_;
    ScopedSnapshotT prev_ss_;
    StepsT steps_;
    std::vector<ID_TYPE> ids_;
    OpStatus status_ = OP_PENDING;
};

Operations::Operations(const OperationContext& context, ScopedSnapshotT prev_ss)
    : context_(context), prev_ss_(prev_ss) {}

Operations::Operations(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id) :
    context_(context), prev_ss_(Snapshots::GetInstance().GetSnapshot(collection_id, commit_id)) {
}

template<typename StepT>
void
Operations::AddStep(const StepT& step) {
    steps_.push_back(std::make_shared<StepT>(step));
}

bool
Operations::IsStale() const {
    auto curr_ss = Snapshots::GetInstance().GetSnapshot(prev_ss_->GetCollectionId());
    if (prev_ss_->GetID() == curr_ss->GetID()) {
        return false;
    }

    return true;
}

void
Operations::OnExecute() {
    auto r = PreExecute();
    if (!r) {
        status_ = OP_FAIL_FLUSH_META;
        return;
    }
    r = DoExecute();
    if (!r) {
        status_ = OP_FAIL_FLUSH_META;
        return;
    }
    PostExecute();
}

bool
Operations::PreExecute() {
    return true;
}

bool
Operations::DoExecute() {
    return true;
}

bool
Operations::PostExecute() {
    /* std::cout << "Operations " << Name << " is OnExecute with " << steps_.size() << " steps" << std::endl; */
    auto& store = Store::GetInstance();
    auto ok = store.DoCommitOperation(*this);
    if (!ok) status_ = OP_FAIL_FLUSH_META;
    return ok;
}

template <typename ResourceT>
class CommitOperation : public Operations {
public:
    using BaseT = Operations;
    CommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    CommitOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    virtual typename ResourceT::Ptr GetPrevResource() const {
        return nullptr;
    }

    bool DoExecute() override {
        auto prev_resource = GetPrevResource();
        if (!prev_resource) return false;
        resource_ = std::make_shared<ResourceT>(*prev_resource);
        resource_->SetID(0);
        AddStep(*resource_);
        return true;
    }

    typename ResourceT::Ptr GetResource() const  {
        if (status_ == OP_PENDING) return nullptr;
        if (ids_.size() == 0) return nullptr;
        resource_->SetID(ids_[0]);
        return resource_;
    }

protected:
    typename ResourceT::Ptr resource_;
};

/* template <typename ResourceT> */
/* class DeltaMappingsCommitOperation : public CommitOperation<ResourceT> { */
/* public: */
/*     using BaseT = CommitOperation<ResourceT>; */
/* }; */

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
        /* return prev_ss_->GetPartitionCommit(context_.new_segment_commit->GetPartitionId()); */
    }

    bool DoExecute() override {
        auto prev_resource = GetPrevResource();
        if (!prev_resource) return false;
        resource_ = std::make_shared<PartitionCommit>(*prev_resource);
        auto prev_segment_commit = prev_ss_->GetSegmentCommit(
                context_.new_segment_commit->GetSegmentId());
        resource_->GetMappings().erase(prev_segment_commit->GetID());
        resource_->GetMappings().insert(context_.new_segment_commit->GetID());
        resource_->SetID(0);
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
        return prev_ss_->GetSegmentCommit(context_.new_segment_file->GetSegmentId());
    }
};

class SegmentFileOperation : public CommitOperation<SegmentFile> {
public:
    using BaseT = CommitOperation<SegmentFile>;
    SegmentFileOperation(const OperationContext& context, ScopedSnapshotT prev_ss, const SegmentFileContext& sc)
    /* SegmentFileOperation(ScopedSnapshotT prev_ss, OperationContext context) */
        : BaseT(context, prev_ss), context_(sc) {};
    SegmentFileOperation(const OperationContext& context, const SegmentFileContext& sc, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id), context_(sc) {};

    bool DoExecute() override;

protected:
    SegmentFileContext context_;
};

bool
SegmentFileOperation::DoExecute() {

    auto field_element_id = prev_ss_->GetFieldElementId(context_.field_name, context_.field_element_name);
    resource_ = std::make_shared<SegmentFile>(context_.partition_id, context_.segment_id, field_element_id);
    /* resource_ = std::make_shared<SegmentFile>(context_.prev_segment->GetPartitionId(), */
    /*         context_.prev_segment->GetID(), */
    /*         context_.prev_field_element->GetID()); */
    AddStep(*resource_);
    return true;
}

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
};

bool
BuildOperation::PreExecute() {
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

    AddStep(*context_.new_segment_file);
    AddStep(*context_.new_segment_commit);
    AddStep(*pc_op.GetResource());
    AddStep(*cc_op.GetResource());
    return true;
}

bool
BuildOperation::DoExecute() {
    if (status_ != OP_PENDING) {
        return false;
    }
    if (IsStale()) {
        status_ = OP_STALE_CANCEL;
        return false;
    }
    /* if (!prev_ss_->HasFieldElement(context_.field_name, context_.field_element_name)) { */
    /*     status_ = OP_FAIL_INVALID_PARAMS; */
    /*     return false; */
    /* } */

    // PXU TODO: Temp comment below check for test
    /* if (prev_ss_->HasSegmentFile(context_.field_name, context_.field_element_name, context_.segment_id)) { */
    /*     status_ = OP_FAIL_DUPLICATED; */
    /*     return; */
    /* } */

    if (IsStale()) {
        status_ = OP_STALE_CANCEL;
        // PXU TODO: Produce cleanup job
        return false;
    }
    std::any_cast<SegmentFilePtr>(steps_[0])->Activate();
    std::any_cast<SegmentCommitPtr>(steps_[1])->Activate();
    std::any_cast<PartitionCommitPtr>(steps_[2])->Activate();
    std::any_cast<CollectionCommitPtr>(steps_[3])->Activate();
    return true;
}
