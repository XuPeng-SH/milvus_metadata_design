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

#include "BaseResource.h"
#include "db/Utils.h"
#include "ResourceTypes.h"
#include "ScopedResource.h"
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <thread>

using milvus::engine::utils::GetMicroSecTimeStamp;

namespace milvus {
namespace engine {
namespace snapshot {

class MappingsField {
public:
    MappingsField(const MappingT& mappings = {}) : mappings_(mappings) {
    }
    const MappingT& GetMappings() const { return mappings_; }
    MappingT& GetMappings() { return mappings_; }

protected:
    MappingT mappings_;
};

class StatusField {
public:
    StatusField(State status = PENDING) : status_(status) {}
    State GetStatus() const {return status_;}

    bool IsActive() const {return status_ == ACTIVE;}
    bool IsDeactive() const {return status_ == DEACTIVE;}

    bool Activate() {
        if (IsDeactive()) return false;
        status_ = ACTIVE;
        return true;
    }

    void Deactivate() {
        status_ = DEACTIVE;
    }

    void ResetStatus() {
        status_ = PENDING;
    }

protected:
    State status_;
};

class CreatedOnField {
public:
    CreatedOnField(TS_TYPE created_on = GetMicroSecTimeStamp()) : created_on_(created_on) {}
    TS_TYPE GetCreatedTime() const {return created_on_;}

protected:
    TS_TYPE created_on_;
};

class IdField {
public:
    IdField(ID_TYPE id) : id_(id) {}
    ID_TYPE GetID() const { return id_; };
    void SetID(ID_TYPE id) {id_ = id;}
    bool HasAssigned() { return id_ > 0; }

protected:
    ID_TYPE id_;
};

class CollectionIdField {
public:
    CollectionIdField(ID_TYPE id) : collection_id_(id) {}
    ID_TYPE GetCollectionId() const { return collection_id_; };

protected:
    ID_TYPE collection_id_;
};

class SchemaIdField {
public:
    SchemaIdField(ID_TYPE id) : schema_id_(id) {}
    ID_TYPE GetSchemaId() const { return schema_id_; };
    void SetSchemaId(ID_TYPE schema_id) { schema_id_ = schema_id; };

protected:
    ID_TYPE schema_id_;
};

class NumField {
public:
    NumField(NUM_TYPE num) : num_(num) {}
    NUM_TYPE GetNum() const { return num_; };
    void SetNum(NUM_TYPE num) { num_ = num; }

protected:
    NUM_TYPE num_;
};

class FtypeField {
public:
    FtypeField(FTYPE_TYPE type) : ftype_(type) {}
    FTYPE_TYPE GetFtype() const { return ftype_; };

protected:
    FTYPE_TYPE ftype_;
};

class FieldIdField {
public:
    FieldIdField(ID_TYPE id) : field_id_(id) {}
    ID_TYPE GetFieldId() const { return field_id_; };

protected:
    ID_TYPE field_id_;
};

class FieldElementIdField {
public:
    FieldElementIdField(ID_TYPE id) : field_element_id_(id) {}
    ID_TYPE GetFieldElementId() const { return field_element_id_; };

protected:
    ID_TYPE field_element_id_;
};

class PartitionIdField {
public:
    PartitionIdField(ID_TYPE id) : partition_id_(id) {}
    ID_TYPE GetPartitionId() const { return partition_id_; };

protected:
    ID_TYPE partition_id_;
};

class SegmentIdField {
public:
    SegmentIdField(ID_TYPE id) : segment_id_(id) {}
    ID_TYPE GetSegmentId() const { return segment_id_; };

protected:
    ID_TYPE segment_id_;
};

class NameField {
public:
    NameField(const std::string& name) : name_(name) {}
    const std::string& GetName() const { return name_; };

protected:
    std::string name_;
};

class Collection : public DBBaseResource<NameField, IdField, StatusField, CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<Collection>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    using BaseT = DBBaseResource<NameField, IdField, StatusField, CreatedOnField>;
    static constexpr const char* Name = "Collection";

    Collection(const std::string& name, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

};

using CollectionPtr = Collection::Ptr;

class SchemaCommit : public DBBaseResource<CollectionIdField,
                                           MappingsField,
                                           IdField,
                                           StatusField,
                                           CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<SchemaCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    using BaseT = DBBaseResource<CollectionIdField, MappingsField, IdField, StatusField, CreatedOnField>;
    static constexpr const char* Name = "SchemaCommit";

    SchemaCommit(ID_TYPE collection_id, const MappingT& mappings = {}, ID_TYPE id = 0,
            State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp());
};

using SchemaCommitPtr = SchemaCommit::Ptr;

class Field : public DBBaseResource<NameField,
                                    NumField,
                                    IdField,
                                    StatusField,
                                    CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<Field>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    using BaseT = DBBaseResource<NameField, NumField, IdField, StatusField, CreatedOnField>;
    static constexpr const char* Name = "Field";

    Field(const std::string& name, NUM_TYPE num, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());
};

using FieldPtr = Field::Ptr;

class FieldCommit : public DBBaseResource<CollectionIdField,
                                          FieldIdField,
                                          MappingsField,
                                          IdField,
                                          StatusField,
                                          CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<FieldCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    using BaseT = DBBaseResource<CollectionIdField, FieldIdField, MappingsField,
          IdField, StatusField, CreatedOnField>;
    static constexpr const char* Name = "FieldCommit";

    FieldCommit(ID_TYPE collection_id, ID_TYPE field_id, const MappingT& mappings = {}, ID_TYPE id = 0,
            State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp());
};

using FieldCommitPtr = FieldCommit::Ptr;

class FieldElement : public DBBaseResource<CollectionIdField,
                                           FieldIdField,
                                           NameField,
                                           FtypeField,
                                           IdField,
                                           StatusField,
                                           CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<FieldElement>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "FieldElement";
    using BaseT = DBBaseResource<CollectionIdField, FieldIdField, NameField,
          FtypeField, IdField, StatusField, CreatedOnField>;
    FieldElement(ID_TYPE collection_id, ID_TYPE field_id, const std::string& name, FTYPE_TYPE ftype,
            ID_TYPE id = 0, State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp());
};

using FieldElementPtr = FieldElement::Ptr;

class CollectionCommit : public DBBaseResource<CollectionIdField,
                                               SchemaIdField,
                                               MappingsField,
                                               IdField,
                                               StatusField,
                                               CreatedOnField>
{
public:
    static constexpr const char* Name = "CollectionCommit";
    using Ptr = std::shared_ptr<CollectionCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    using BaseT = DBBaseResource<CollectionIdField, SchemaIdField, MappingsField,
          IdField, StatusField, CreatedOnField>;
    CollectionCommit(ID_TYPE collection_id, ID_TYPE schema_id, const MappingT& mappings = {}, ID_TYPE id = 0,
            State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp());
};

using CollectionCommitPtr = CollectionCommit::Ptr;

class Partition : public DBBaseResource<NameField,
                                        CollectionIdField,
                                        IdField,
                                        StatusField,
                                        CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<Partition>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "Partition";
    using BaseT = DBBaseResource<NameField, CollectionIdField, IdField, StatusField, CreatedOnField>;

    Partition(const std::string& name, ID_TYPE collection_id, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());
};

using PartitionPtr = Partition::Ptr;

class PartitionCommit : public DBBaseResource<CollectionIdField,
                                              PartitionIdField,
                                              MappingsField,
                                              IdField,
                                              StatusField,
                                              CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<PartitionCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "PartitionCommit";
    using BaseT = DBBaseResource<CollectionIdField, PartitionIdField, MappingsField,
          IdField, StatusField, CreatedOnField>;
    PartitionCommit(ID_TYPE collection_id, ID_TYPE partition_id,
            const MappingT& mappings = {}, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    std::string ToString() const override;
};

using PartitionCommitPtr = PartitionCommit::Ptr;

class Segment : public DBBaseResource<PartitionIdField,
                                      NumField,
                                      IdField,
                                      StatusField,
                                      CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<Segment>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "Segment";
    using BaseT = DBBaseResource<PartitionIdField, NumField, IdField, StatusField, CreatedOnField>;

    Segment(ID_TYPE partition_id, ID_TYPE num = 0, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    std::string ToString() const override;
};

using SegmentPtr = Segment::Ptr;

class SegmentCommit : public DBBaseResource<SchemaIdField,
                                            PartitionIdField,
                                            SegmentIdField,
                                            MappingsField,
                                            IdField,
                                            StatusField,
                                            CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<SegmentCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "SegmentCommit";
    using BaseT = DBBaseResource<SchemaIdField, PartitionIdField, SegmentIdField,
          MappingsField, IdField, StatusField, CreatedOnField>;
    SegmentCommit(ID_TYPE schema_id, ID_TYPE partition_id, ID_TYPE segment_id,
            const MappingT& mappings = {}, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    std::string ToString() const override;
};

using SegmentCommitPtr = SegmentCommit::Ptr;

class SegmentFile : public DBBaseResource<PartitionIdField,
                                          SegmentIdField,
                                          FieldElementIdField,
                                          IdField,
                                          StatusField,
                                          CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<SegmentFile>;
    using MapT = std::map<ID_TYPE, Ptr>;
    using VecT = std::vector<Ptr>;
    static constexpr const char* Name = "SegmentFile";
    using BaseT = DBBaseResource<PartitionIdField, SegmentIdField, FieldElementIdField, IdField,
          StatusField, CreatedOnField>;

    SegmentFile(ID_TYPE partition_id, ID_TYPE segment_id, ID_TYPE field_element_id, ID_TYPE id = 0,
            State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp());
};

using SegmentFilePtr = SegmentFile::Ptr;

} // snapshot
} // engine
} // milvus
