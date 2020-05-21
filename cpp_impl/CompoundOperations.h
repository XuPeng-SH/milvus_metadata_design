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

#include "ResourceOperations.h"
#include "Snapshot.h"

namespace milvus {
namespace engine {
namespace snapshot {

class BuildOperation : public Operations {
public:
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

class MergeOperation : public Operations {
public:
    using BaseT = Operations;

    MergeOperation(const OperationContext& context, ScopedSnapshotT prev_ss);
    MergeOperation(const OperationContext& context, ID_TYPE collection_id, ID_TYPE commit_id = 0);

    bool PreExecute() override;
    bool DoExecute() override;

    SegmentPtr NewSegment();
    SegmentFilePtr NewSegmentFile(const SegmentFileContext& context);
};

} // snapshot
} // engine
} // milvus
