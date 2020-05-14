#pragma once

#include "Helper.h"
#include "Schema.h"
#include "Proxy.h"
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <thread>


class FieldMixin {
public:

protected:
    void InstallField(const std::string& field);

    std::vector<std::string> fields_;
};

template <typename ...Fields>
class DBBaseResource : public ReferenceProxy,
                       public FieldMixin,
                       public Fields... {
public:
    DBBaseResource(const Fields&... fields);

    virtual std::string ToString() const;

    virtual ~DBBaseResource() {}
};

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

protected:
    ID_TYPE schema_id_;
};

class NumField {
public:
    NumField(NUM_TYPE num) : num_(num) {}
    NUM_TYPE GetNum() const { return num_; };

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
    using BaseT = DBBaseResource<NameField, IdField, StatusField, CreatedOnField>;
    static constexpr const char* Name = "Collection";

    Collection(const std::string& name, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

};

using CollectionPtr = std::shared_ptr<Collection>;

class SchemaCommit : public DBBaseResource<CollectionIdField,
                                           MappingsField,
                                           IdField,
                                           StatusField,
                                           CreatedOnField>
{
public:
    using Ptr = std::shared_ptr<SchemaCommit>;
    using MapT = std::map<ID_TYPE, Ptr>;
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
    using BaseT = DBBaseResource<NameField, NumField, IdField, StatusField, CreatedOnField>;
    static constexpr const char* Name = "Field";

    Field(const std::string& name, NUM_TYPE num, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());
};

using FieldPtr = std::shared_ptr<Field>;

class FieldCommit : public DBBaseResource<CollectionIdField,
                                          FieldIdField,
                                          MappingsField,
                                          IdField,
                                          StatusField,
                                          CreatedOnField>
{
public:
    using BaseT = DBBaseResource<CollectionIdField, FieldIdField, MappingsField,
          IdField, StatusField, CreatedOnField>;
    static constexpr const char* Name = "FieldCommit";

    FieldCommit(ID_TYPE collection_id, ID_TYPE field_id, const MappingT& mappings = {}, ID_TYPE id = 0,
            State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp());
};

using FieldCommitPtr = std::shared_ptr<FieldCommit>;

class FieldElement : public DBBaseResource<CollectionIdField,
                                           FieldIdField,
                                           NameField,
                                           FtypeField,
                                           IdField,
                                           StatusField,
                                           CreatedOnField>
{
public:
    static constexpr const char* Name = "FieldElement";
    using BaseT = DBBaseResource<CollectionIdField, FieldIdField, NameField,
          FtypeField, IdField, StatusField, CreatedOnField>;
    FieldElement(ID_TYPE collection_id, ID_TYPE field_id, const std::string& name, FTYPE_TYPE ftype,
            ID_TYPE id = 0, State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp());
};

using FieldElementPtr = std::shared_ptr<FieldElement>;

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
    static constexpr const char* Name = "Partition";
    using BaseT = DBBaseResource<NameField, CollectionIdField, IdField, StatusField, CreatedOnField>;

    Partition(const std::string& name, ID_TYPE collection_id, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());
};

using PartitionPtr = std::shared_ptr<Partition>;

class PartitionCommit : public DBBaseResource<CollectionIdField,
                                              PartitionIdField,
                                              MappingsField,
                                              IdField,
                                              StatusField,
                                              CreatedOnField>
{
public:
    static constexpr const char* Name = "PartitionCommit";
    using BaseT = DBBaseResource<CollectionIdField, PartitionIdField, MappingsField,
          IdField, StatusField, CreatedOnField>;
    PartitionCommit(ID_TYPE collection_id, ID_TYPE partition_id,
            const MappingT& mappings = {}, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());
};

using PartitionCommitPtr = std::shared_ptr<PartitionCommit>;

class Segment : public DBBaseResource<PartitionIdField,
                                      IdField,
                                      StatusField,
                                      CreatedOnField>
{
public:
    static constexpr const char* Name = "Segment";
    using BaseT = DBBaseResource<PartitionIdField, IdField, StatusField, CreatedOnField>;

    Segment(ID_TYPE partition_id, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());
};

using SegmentPtr = std::shared_ptr<Segment>;

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
    static constexpr const char* Name = "SegmentCommit";
    using BaseT = DBBaseResource<SchemaIdField, PartitionIdField, SegmentIdField,
          MappingsField, IdField, StatusField, CreatedOnField>;
    SegmentCommit(ID_TYPE schema_id, ID_TYPE partition_id, ID_TYPE segment_id,
            const MappingT& mappings = {}, ID_TYPE id = 0, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());
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
    static constexpr const char* Name = "SegmentFile";
    using BaseT = DBBaseResource<PartitionIdField, SegmentIdField, FieldElementIdField, IdField,
          StatusField, CreatedOnField>;

    SegmentFile(ID_TYPE partition_id, ID_TYPE segment_id, ID_TYPE field_element_id, ID_TYPE id = 0,
            State status = PENDING, TS_TYPE created_on = GetMicroSecTimeStamp());
};

using SegmentFilePtr = std::shared_ptr<SegmentFile>;

#include "Resources.inl"
