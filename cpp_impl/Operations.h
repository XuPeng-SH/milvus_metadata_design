#pragma once

#include "Snapshots.h"
#include "Store.h"
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

template <typename Derived>
class Operations {
public:
    static constexpr const char* Name = Derived::Name;
    Operations(ScopedSnapshotT prev_ss);
    Operations(ID_TYPE collection_id, ID_TYPE commit_id = 0);

    const ScopedSnapshotT& GetPrevSnapshot() const {return prev_ss_;}

    template<typename StepT>
    void AddStep(const StepT& step);

    StepsT& GetSteps() { return steps_; }

    virtual void OnExecute();

protected:
    ScopedSnapshotT prev_ss_;
    StepsT steps_;
};

template <typename Derived>
Operations<Derived>::Operations(ScopedSnapshotT prev_ss) : prev_ss_(prev_ss) {}

template <typename Derived>
Operations<Derived>::Operations(ID_TYPE collection_id, ID_TYPE commit_id) :
    prev_ss_(Snapshots::GetInstance().GetSnapshot(collection_id, commit_id)) {
}

template <typename Derived>
template<typename StepT>
void
Operations<Derived>::AddStep(const StepT& step) {
    steps_.push_back(std::make_shared<StepT>(step));
    /* auto& container = std::get<Index<typename StepT::VecT, StepsT>::value>(steps_); */
    /* container.push_back(std::make_shared<StepT>(step)); */
}

template <typename Derived>
void
Operations<Derived>::OnExecute() {
    std::cout << "Operations " << Name << " is OnExecute with " << steps_.size() << " steps" << std::endl;
    auto& store = Store::GetInstance();
    store.DoCommitOperation(*this);
}

class BuildOperation : public Operations<BuildOperation> {
public:
    static constexpr const char* Name = "Build";
    using BaseT = Operations<BuildOperation>;

    BuildOperation(ScopedSnapshotT prev_ss) : BaseT(prev_ss) {};
    BuildOperation(ID_TYPE collection_id, ID_TYPE commit_id = 0) : BaseT(collection_id, commit_id) {};
};
