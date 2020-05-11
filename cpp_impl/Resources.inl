#include "Resources.h"
#include "Store.h"
#include <sstream>
#include <iostream>


void FieldMixin::InstallField(const std::string& field) {
    fields_.push_back(field);
}

template <typename ...Fields>
DBBaseResource<Fields...>::DBBaseResource(const Fields&... fields) : Fields(fields)... {
    /* InstallField("id"); */
    /* InstallField("status"); */
    /* InstallField("created_on"); */
    /* std::vector<std::string> attrs = {Fields::ATTR...}; */
}

template <typename ...Fields>
std::string DBBaseResource<Fields...>::ToString() const {
    for (auto& field : fields_) {
        std::cout << "Field->" << field << std::endl;
    }
    std::stringstream ss;
    /* ss << "ID=" << id_ << ", Status=" << status_ << ", TS=" << created_on_; */
    return ss.str();
}

Collection::Collection(ID_TYPE id, const std::string& name, State status, TS_TYPE created_on) :
    BaseT(id, name, status, created_on) {
}

CollectionCommit::CollectionCommit(ID_TYPE id, ID_TYPE collection_id,
        const MappingT& mappings, State status, TS_TYPE created_on) :
    BaseT(id, collection_id, mappings, status, created_on) {
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

Partition::Partition(ID_TYPE id, const std::string& name, ID_TYPE collection_id,
        State status, TS_TYPE created_on) :
    BaseT(id, name, collection_id, status, created_on)
{
}

PartitionCommit::PartitionCommit(ID_TYPE id, ID_TYPE collection_id, ID_TYPE partition_id,
        const MappingT& mappings, State status, TS_TYPE created_on) :
    BaseT(id, collection_id, partition_id, mappings, status, created_on) {
}

Segment::Segment(ID_TYPE id, ID_TYPE partition_id, State status, TS_TYPE created_on) :
    BaseT(id, partition_id, status, created_on)
{
}

SegmentCommit::SegmentCommit(ID_TYPE id, ID_TYPE schema_id, ID_TYPE partition_id, ID_TYPE segment_id,
        const MappingT& mappings, State status, TS_TYPE created_on) :
    BaseT(id, schema_id, partition_id, segment_id, mappings, status, created_on) {
}

SegmentFile::SegmentFile(ID_TYPE id, ID_TYPE partition_id, ID_TYPE segment_id, ID_TYPE field_element_id,
            State status, TS_TYPE created_on) :
    BaseT(id, partition_id, segment_id, field_element_id, status, created_on) {
}
