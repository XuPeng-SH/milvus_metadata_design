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
    /* std::cout << "Operations " << Name << " is OnExecute with " << steps_.size() << " steps" << std::endl; */
    auto& store = Store::GetInstance();
    auto ok = store.DoCommitOperation(*this);
    if (!ok) status_ = OP_FAIL_FLUSH_META;
}

class NewSegmentFileOperation : public Operations {
public:
    using BaseT = Operations;
    NewSegmentFileOperation(ScopedSnapshotT prev_ss, const BuildContext& context)
        : BaseT(prev_ss), context_(context) {};
    NewSegmentFileOperation(const BuildContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), context_(context) {};

    void OnExecute() override;

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

void
NewSegmentFileOperation::OnExecute() {
    auto field_element_id = prev_ss_->GetFieldElementId(context_.field_name, context_.field_element_name);
    auto sf = SegmentFile(context_.partition_id, context_.segment_id, field_element_id);
    AddStep(sf);
    BaseT::OnExecute();
}

class BuildOperation : public Operations {
public:
    static constexpr const char* Name = "Build";
    using BaseT = Operations;

    BuildOperation(ScopedSnapshotT prev_ss, const BuildContext& context)
        : BaseT(prev_ss), context_(context) {};
    BuildOperation(const BuildContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(collection_id, commit_id), context_(context) {};

    void OnExecute() override;

protected:
    BuildContext context_;
};

void
BuildOperation::OnExecute() {
    if (status_ != OP_PENDING) {
        return;
    }
    if (IsStale()) {
        status_ = OP_STALE_CANCEL;
        return;
    }
    if (!prev_ss_->HasFieldElement(context_.field_name, context_.field_element_name)) {
        status_ = OP_FAIL_INVALID_PARAMS;
        return;
    }

    // PXU TODO: Temp comment below check for test
    /* if (prev_ss_->HasSegmentFile(context_.field_name, context_.field_element_name, context_.segment_id)) { */
    /*     status_ = OP_FAIL_DUPLICATED; */
    /*     return; */
    /* } */

    auto& new_segment_file = steps_[0];
    if (new_segment_file.type() != typeid(SegmentFile::Ptr)) {
        status_ = OP_FAIL_INVALID_PARAMS;
        return;
    }

    if (IsStale()) {
        status_ = OP_STALE_CANCEL;
        // PXU TODO: Produce cleanup job
        return;
    }

    BaseT::OnExecute();
}
