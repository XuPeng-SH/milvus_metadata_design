#pragma once

#include "Resources.h"
#include <string>

struct SegmentFileContext {
    std::string field_name;
    std::string field_element_name;
    ID_TYPE segment_id;
    ID_TYPE partition_id;
};

struct OperationContext {
    SegmentFilePtr new_segment_file = nullptr;
    SegmentCommitPtr new_segment_commit = nullptr;
    PartitionCommitPtr new_partition_commit = nullptr;
    SchemaCommitPtr new_schema_commit = nullptr;

    FieldPtr prev_field = nullptr;
    FieldElementPtr prev_field_element = nullptr;

    SegmentPtr prev_segment = nullptr;
    SegmentCommitPtr prev_segment_commit = nullptr;
    PartitionPtr prev_partition = nullptr;
    PartitionCommitPtr prev_partition_commit = nullptr;
    CollectionCommitPtr prev_collection_commit = nullptr;
};

struct BuildContext {
    SegmentFilePtr new_segment_file = nullptr;
    SegmentCommitPtr new_segment_commit = nullptr;
};

struct CollectionCommitContext {
    PartitionCommitPtr new_partition_commit = nullptr;
    SchemaCommitPtr new_schema_commit = nullptr;
};
