#pragma once

#include "Resources.h"
#include <string>
#include <iostream>

struct SegmentFileContext {
    std::string field_name;
    std::string field_element_name;
    ID_TYPE segment_id;
    ID_TYPE partition_id;
};

struct OperationContext {
    /* OperationContext(const OperationContext& other) { */
    /*     new_segment_file = other.new_segment_file; */
    /*     new_segment_commit = other.new_segment_commit; */
    /*     new_partition_commit = other.new_partition_commit; */
    /*     new_schema_commit = other.new_schema_commit; */
    /*     prev_field = other.prev_field; */
    /*     prev_field_element = other.prev_field_element; */
    /*     prev_segment = other.prev_segment; */
    /*     prev_segment_commit = other.prev_segment_commit; */
    /*     prev_partition = other.prev_partition; */
    /*     prev_partition_commit = other.prev_partition_commit; */
    /*     prev_collection_commit = other.prev_collection_commit; */
    /* } */

    /* OperationContext() {} */

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
