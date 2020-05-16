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

    typename ResourceT::Ptr GetResource() const  {
        if (status_ == OP_PENDING) return nullptr;
        if (ids_.size() == 0) return nullptr;
        resource_->SetID(ids_[0]);
        return resource_;
    }

protected:
    typename ResourceT::Ptr resource_;
};

class NewSegmentCommitOperation : public CommitOperation<SegmentCommit> {
public:
    using BaseT = CommitOperation<SegmentCommit>;
    NewSegmentCommitOperation(ScopedSnapshotT prev_ss, SegmentFilePtr segment_file)
        : BaseT(prev_ss), segment_file_(segment_file) {};
    NewSegmentCommitOperation(SegmentFile::Ptr segment_file, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), segment_file_(segment_file) {};

    bool DoExecute() override {
        auto prev_segment_commit = prev_ss_->GetSegmentCommit(segment_file_->GetSegmentId());
        resource_ = std::make_shared<SegmentCommit>(*prev_segment_commit);
        resource_->GetMappings().push_back(segment_file_->GetID());
        resource_->SetID(0);
        AddStep(*resource_);
        return true;
    }

    /* SegmentCommitPtr GetSegmentCommit() const { */
    /*     if (status_ == OP_PENDING) return nullptr; */
    /*     if (ids_.size() == 0) return nullptr; */
    /*     segment_commit_->SetID(ids_[0]); */
    /*     return segment_commit_; */
    /* } */

protected:
    SegmentFilePtr segment_file_;
    /* SegmentCommitPtr segment_commit_; */
};

class NewSegmentFileOperation : public Operations {
public:
    using BaseT = Operations;
    NewSegmentFileOperation(ScopedSnapshotT prev_ss, const BuildContext& context)
        : BaseT(prev_ss), context_(context) {};
    NewSegmentFileOperation(const BuildContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), context_(context) {};

    bool DoExecute() override;

    SegmentFile::Ptr GetSegmentFile() const {
        if (status_ == OP_PENDING) return nullptr;
        if (ids_.size() == 0) return nullptr;
        auto r = std::make_shared<SegmentFile>(*std::any_cast<SegmentFile::Ptr>(steps_[0]));
        r->SetID(ids_[0]);
        return r;
    }

protected:
    BuildContext context_;
};

bool
NewSegmentFileOperation::DoExecute() {
    auto field_element_id = prev_ss_->GetFieldElementId(context_.field_name, context_.field_element_name);
    auto sf = SegmentFile(context_.partition_id, context_.segment_id, field_element_id);
    AddStep(sf);
    return true;
}

class BuildOperation : public Operations {
public:
    static constexpr const char* Name = "Build";
    using BaseT = Operations;

    BuildOperation(ScopedSnapshotT prev_ss, const BuildContext& context)
        : BaseT(prev_ss), context_(context) {};
    BuildOperation(const BuildContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), context_(context) {};

    bool DoExecute() override;

protected:
    BuildContext context_;
};

bool
BuildOperation::DoExecute() {
    if (status_ != OP_PENDING) {
        return false;
    }
    if (IsStale()) {
        status_ = OP_STALE_CANCEL;
        return false;
    }
    if (!prev_ss_->HasFieldElement(context_.field_name, context_.field_element_name)) {
        status_ = OP_FAIL_INVALID_PARAMS;
        return false;
    }

    // PXU TODO: Temp comment below check for test
    /* if (prev_ss_->HasSegmentFile(context_.field_name, context_.field_element_name, context_.segment_id)) { */
    /*     status_ = OP_FAIL_DUPLICATED; */
    /*     return; */
    /* } */

    auto& new_segment_file = steps_[0];
    if (new_segment_file.type() != typeid(SegmentFile::Ptr)) {
        status_ = OP_FAIL_INVALID_PARAMS;
        return false;
    }

    if (IsStale()) {
        status_ = OP_STALE_CANCEL;
        // PXU TODO: Produce cleanup job
        return false;
    }

    std::any_cast<SegmentFilePtr>(new_segment_file)->Activate();
    std::any_cast<SegmentCommitPtr>(steps_[1])->Activate();
    return true;
}
