#include "CompoundOperations.h"

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

    for (auto& new_segment_file : context_.new_segment_files) {
        AddStep(*new_segment_file);
    }
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
