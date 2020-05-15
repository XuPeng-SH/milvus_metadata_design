#pragma once

#include "Snapshots.h"
#include "Store.h"
#include "Context.h"
#include <assert.h>
#include <vector>
#include <any>

using StepsT = std::vector<std::any>;
/* using StepsT = std::tuple<CollectionCommit::VecT, */
/*                                   Collection::VecT, */
/*                                   SchemaCommit::VecT, */
/*                                   FieldCommit::VecT, */
/*                                   Field::VecT, */
/*                                   FieldElement::VecT, */
/*                                   PartitionCommit::VecT, */
/*                                   Partition::VecT, */
/*                                   SegmentCommit::VecT, */
/*                                   Segment::VecT, */
/*                                   SegmentFile::VecT>; */

class Operations {
public:
    /* static constexpr const char* Name = Derived::Name; */
    Operations(ScopedSnapshotT prev_ss);
    Operations(ID_TYPE collection_id, ID_TYPE commit_id = 0);

    const ScopedSnapshotT& GetPrevSnapshot() const {return prev_ss_;}

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

void
Operations::OnExecute() {
    /* std::cout << "Operations " << Name << " is OnExecute with " << steps_.size() << " steps" << std::endl; */
    auto& store = Store::GetInstance();
    store.DoCommitOperation(*this);
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
    BaseT::OnExecute();
}
