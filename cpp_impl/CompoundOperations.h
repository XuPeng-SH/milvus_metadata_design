#pragma once

#include "ResourceOperations.h"

class BuildOperation : public Operations {
public:
    static constexpr const char* Name = "Build";
    using BaseT = Operations;

    BuildOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    BuildOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    bool DoExecute() override;
    bool PreExecute() override;

    SegmentFilePtr NewSegmentFile(const SegmentFileContext& context);
};

class NewSegmentOperation : public Operations {
public:
    using BaseT = Operations;

    NewSegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    NewSegmentOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    bool DoExecute() override;

    bool PreExecute() override;

    SegmentPtr NewSegment();

    SegmentFilePtr NewSegmentFile(const SegmentFileContext& context);

};
