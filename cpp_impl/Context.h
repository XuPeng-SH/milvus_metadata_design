#pragma once

#include "Resources.h"
#include <string>

struct SegmentFileContext {
    std::string field_name;
    std::string field_element_name;
    ID_TYPE segment_id;
    ID_TYPE partition_id;
};

struct BuildContext {
    SegmentFilePtr new_segment_file = nullptr;
    SegmentCommitPtr new_segment_commit = nullptr;
};

struct CollectionCommitContext {
    PartitionCommitPtr new_partition_commit = nullptr;
    SchemaCommitPtr new_schema_commit = nullptr;
};
