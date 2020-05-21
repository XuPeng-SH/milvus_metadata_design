// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once
#include "Operations.h"

namespace milvus {
namespace engine {
namespace snapshot {

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

    bool DoExecute(Store&) override;
};

/*
 * Context: new_segment_commit@requried stale_segments@optional
 */
class PartitionCommitOperation : public CommitOperation<PartitionCommit> {
public:
    using BaseT = CommitOperation<PartitionCommit>;
    PartitionCommitOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    PartitionCommitOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    PartitionCommitPtr GetPrevResource() const override {
        auto& segment_commit = context_.new_segment_commit;
        return prev_ss_->GetPartitionCommitByPartitionId(segment_commit->GetPartitionId());
    }

    bool DoExecute(Store&) override;
};

/*
 * Context: new_segment_files@requried stale_segment_file@optional
 */
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

    bool DoExecute(Store&) override;
};

/*
 * Context: prev_partition@requried
 */
class SegmentOperation : public CommitOperation<Segment> {
public:
    using BaseT = CommitOperation<Segment>;
    SegmentOperation(const OperationContext& context, ScopedSnapshotT prev_ss)
        : BaseT(context, prev_ss) {};
    SegmentOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0)
        : BaseT(context, collection_id, commit_id) {};

    bool DoExecute(Store& store) override {
        if (!context_.prev_partition) {
            return false;
        }
        auto prev_num = prev_ss_->GetMaxSegmentNumByPartition(context_.prev_partition->GetID());
        resource_ = std::make_shared<Segment>(context_.prev_partition->GetID(), prev_num+1);
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

    bool DoExecute(Store& store) override;

protected:
    SegmentFileContext context_;
};

template <>
class LoadOperation<Collection> : public Operations {
public:
    LoadOperation(const LoadOperationContext& context) :
       Operations(OperationContext(), ScopedSnapshotT()), context_(context) {}

    void ApplyToStore(Store& store) override {
        if (status_ != OP_PENDING) return;
        if (context_.id == 0 && context_.name != "") {
            resource_ = store.GetCollection(context_.name);
        } else {
            resource_ = store.GetResource<Collection>(context_.id);
        }
        Done();
    }

    CollectionPtr GetResource() const  {
        if (status_ == OP_PENDING) return nullptr;
        return resource_;
    }

protected:
    LoadOperationContext context_;
    CollectionPtr resource_;
};

} // snapshot
} // engine
} // milvus
