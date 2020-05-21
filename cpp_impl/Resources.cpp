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

#include "Resources.h"
#include "Store.h"
#include <sstream>
#include <iostream>

namespace milvus {
namespace engine {
namespace snapshot {


Collection::Collection(const std::string& name, ID_TYPE id, State status, TS_TYPE created_on) :
    BaseT(name, id, status, created_on) {
}

SchemaCommit::SchemaCommit(ID_TYPE collection_id, const MappingT& mappings,
       ID_TYPE id, State status, TS_TYPE created_on) :
    BaseT(collection_id, mappings, id, status, created_on) {
}

FieldCommit::FieldCommit(ID_TYPE collection_id, ID_TYPE field_id, const MappingT& mappings,
        ID_TYPE id, State status, TS_TYPE created_on) :
    BaseT(collection_id, field_id, mappings, id, status, created_on) {
}

Field::Field(const std::string& name, NUM_TYPE num, ID_TYPE id, State status, TS_TYPE created_on) :
    BaseT(name, num, id, status, created_on) {

}

FieldElement::FieldElement(ID_TYPE collection_id, ID_TYPE field_id, const std::string& name,
        FTYPE_TYPE ftype, ID_TYPE id, State status, TS_TYPE created_on) :
    BaseT(collection_id, field_id, name, ftype, id, status, created_on) {
}

CollectionCommit::CollectionCommit(ID_TYPE collection_id, ID_TYPE schema_id,
        const MappingT& mappings, ID_TYPE id, State status, TS_TYPE created_on) :
    BaseT(collection_id, schema_id, mappings, id, status, created_on) {
}

/* std::string CollectionCommit::ToString() const { */
/*     std::stringstream ss; */
/*     ss << "<" << BaseT::ToString() << ", Mappings=" << "["; */
/*     bool first = true; */
/*     std::string prefix; */
/*     for (auto& id : mappings_) { */
/*         if (!first) prefix = ", "; */
/*         else first = false; */
/*         ss << prefix << id; */
/*     } */
/*     ss << "]>"; */
/*     return ss.str(); */
/* } */

Partition::Partition(const std::string& name, ID_TYPE collection_id, ID_TYPE id,
        State status, TS_TYPE created_on) :
    BaseT(name, collection_id, id, status, created_on)
{
}

PartitionCommit::PartitionCommit(ID_TYPE collection_id, ID_TYPE partition_id,
        const MappingT& mappings, ID_TYPE id, State status, TS_TYPE created_on) :
    BaseT(collection_id, partition_id, mappings, id, status, created_on) {
}

std::string
PartitionCommit::ToString() const {
    std::stringstream ss;
    ss << "PartitionCommit [" << this << "]: ";
    ss << "id=" << GetID() << ", ";
    ss << "partition_id=" << GetPartitionId() << ", mappings=(";
    for(auto sc_id : GetMappings()) {
        ss << sc_id << ", ";
    }
    ss << ") status=" << GetStatus() << " ";
    return ss.str();
}

Segment::Segment(ID_TYPE partition_id, ID_TYPE num, ID_TYPE id, State status, TS_TYPE created_on) :
    BaseT(partition_id, num, id, status, created_on)
{
}

std::string
Segment::ToString() const {
    std::stringstream ss;
    ss << "Segment [" << this << "]: ";
    ss << "id=" << GetID() << ", ";
    ss << "partition_id=" << GetPartitionId() << ", ";
    ss << "num=" << (NUM_TYPE)GetNum() << ", ";
    ss << "status=" << GetStatus() << ", ";
    return ss.str();
}

SegmentCommit::SegmentCommit(ID_TYPE schema_id, ID_TYPE partition_id, ID_TYPE segment_id,
        const MappingT& mappings, ID_TYPE id, State status, TS_TYPE created_on) :
    BaseT(schema_id, partition_id, segment_id, mappings, id, status, created_on) {
}

std::string
SegmentCommit::ToString() const {
    std::stringstream ss;
    ss << "SegmentCommit [" << this << "]: ";
    ss << "id=" << GetID() << ", ";
    ss << "partition_id=" << GetPartitionId() << ", ";
    ss << "segment_id=" << GetSegmentId() << ", ";
    ss << "status=" << GetStatus() << ", ";
    return ss.str();
}

SegmentFile::SegmentFile(ID_TYPE partition_id, ID_TYPE segment_id, ID_TYPE field_element_id, ID_TYPE id,
            State status, TS_TYPE created_on) :
    BaseT(partition_id, segment_id, field_element_id, id, status, created_on) {
}

} // snapshot
} // engine
} // milvus
