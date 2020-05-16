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
    Operations(ScopedSnapshotT prev_ss);
    Operations(ID_TYPE collection_id, ID_TYPE commit_id = 0);

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
    ScopedSnapshotT prev_ss_;
    StepsT steps_;
    std::vector<ID_TYPE> ids_;
    OpStatus status_ = OP_PENDING;
};

Operations::Operations(ScopedSnapshotT prev_ss) : prev_ss_(prev_ss) {}

Operations::Operations(ID_TYPE collection_id, ID_TYPE commit_id) :
    prev_ss_(Snapshots::GetInstance().GetSnapshot(collection_id, commit_id)) {
}

template<typename StepT>
void
Operations::AddStep(const StepT& step) {
    steps_.push_back(std::make_shared<StepT>(step));
    /* auto& container = std::get<Index<typename StepT::VecT, StepsT>::value>(steps_); */
    /* container.push_back(std::make_shared<StepT>(step)); */
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
    CommitOperation(ScopedSnapshotT prev_ss)
        : BaseT(prev_ss) {};
    CommitOperation(ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id) {};

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
    CollectionCommitOperation(ScopedSnapshotT prev_ss, OperationContext context)
        : BaseT(prev_ss), context_(context) {};
    CollectionCommitOperation(OperationContext context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), context_(context) {};

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

protected:
    OperationContext context_;
};

class PartitionCommitOperation : public CommitOperation<PartitionCommit> {
public:
    using BaseT = CommitOperation<PartitionCommit>;
    PartitionCommitOperation(ScopedSnapshotT prev_ss, OperationContext context)
        : BaseT(prev_ss), context_(context) {};
    PartitionCommitOperation(OperationContext context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), context_(context) {};

    PartitionCommitPtr GetPrevResource() const override {
        return prev_ss_->GetPartitionCommit(context_.new_segment_commit->GetPartitionId());
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

protected:
    OperationContext context_;
};

class SegmentCommitOperation : public CommitOperation<SegmentCommit> {
public:
    using BaseT = CommitOperation<SegmentCommit>;
    SegmentCommitOperation(ScopedSnapshotT prev_ss, OperationContext context)
        : BaseT(prev_ss), context_(context) {};
    SegmentCommitOperation(OperationContext context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), context_(context) {};

    SegmentCommit::Ptr GetPrevResource() const override {
        return prev_ss_->GetSegmentCommit(context_.new_segment_file->GetSegmentId());
    }

protected:
    OperationContext context_;
};

class SegmentFileOperation : public CommitOperation<SegmentFile> {
public:
    using BaseT = CommitOperation<SegmentFile>;
    SegmentFileOperation(ScopedSnapshotT prev_ss, const SegmentFileContext& context)
        : BaseT(prev_ss), context_(context) {};
    SegmentFileOperation(const SegmentFileContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), context_(context) {};

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

class BuildOperation : public Operations {
public:
    static constexpr const char* Name = "Build";
    using BaseT = Operations;

    BuildOperation(ScopedSnapshotT prev_ss, const OperationContext& context)
        : BaseT(prev_ss), context_(context) {};
    BuildOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), context_(context) {};

    bool DoExecute() override;
    bool PreExecute() override;

protected:
    OperationContext context_;
};

bool
BuildOperation::PreExecute() {
    SegmentCommitOperation op(prev_ss_, context_);
    op.OnExecute();
    context_.new_segment_commit = op.GetResource();
    if (!context_.new_segment_commit) return false;

    PartitionCommitOperation pc_op(prev_ss_, context_);
    pc_op.OnExecute();

    OperationContext cc_context;
    cc_context.new_partition_commit = pc_op.GetResource();
    CollectionCommitOperation cc_op(prev_ss_, cc_context);
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
