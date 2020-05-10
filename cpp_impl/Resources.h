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

template <typename Derived, typename ...Mixins>
class DBBaseResource : public ReferenceProxy,
                       public FieldMixin,
                       public Mixins... {
public:
    using Ptr = std::shared_ptr<Derived>;
    DBBaseResource(const Mixins&... mixins);

    virtual std::string ToString() const;

    virtual ~DBBaseResource() {}
};

class MappingsMixin {
public:
    MappingsMixin(const MappingT& mappings = {}) : mappings_(mappings) {
    }
    const MappingT& GetMappings() const { return mappings_; }
    MappingT& GetMappings() { return mappings_; }

protected:
    MappingT mappings_;
};

class StatusMixin {
public:
    StatusMixin(State status = PENDING) : status_(status) {}
    State GetStatus() const {return status_;}

    bool IsActive() const {return status_ == ACTIVE;}
    bool IsDeactive() const {return status_ == DEACTIVE;}

protected:
    State status_;
};

class CreatedOnMixin {
public:
    CreatedOnMixin(TS_TYPE created_on = GetMicroSecTimeStamp()) : created_on_(created_on) {}
    TS_TYPE GetCreatedTime() const {return created_on_;}

protected:
    TS_TYPE created_on_;
};

class IdMixin {
public:
    IdMixin(ID_TYPE id) : id_(id) {}
    ID_TYPE GetID() const { return id_; };

protected:
    ID_TYPE id_;
};

class CollectionIdMixin {
public:
    CollectionIdMixin(ID_TYPE id) : collection_id_(id) {}
    ID_TYPE GetCollectionId() const { return collection_id_; };

protected:
    ID_TYPE collection_id_;
};

class PartitionIdMixin {
public:
    PartitionIdMixin(ID_TYPE id) : partition_id_(id) {}
    ID_TYPE GetPartitionId() const { return partition_id_; };

protected:
    ID_TYPE partition_id_;
};

class NameMixin {
public:
    NameMixin(const std::string& name) : name_(name) {}
    const std::string& GetName() const { return name_; };

protected:
    std::string name_;
};


class Collection : public DBBaseResource<Collection,
                                         IdMixin,
                                         NameMixin,
                                         StatusMixin,
                                         CreatedOnMixin>
{
public:
    using BaseT = DBBaseResource<Collection, IdMixin, NameMixin, StatusMixin, CreatedOnMixin>;

    Collection(ID_TYPE id, const std::string& name, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    std::string ToString() const override;
};

using CollectionPtr = std::shared_ptr<Collection>;


class CollectionCommit : public DBBaseResource<CollectionCommit,
                                               IdMixin,
                                               CollectionIdMixin,
                                               MappingsMixin,
                                               StatusMixin,
                                               CreatedOnMixin>
{
public:
    using BaseT = DBBaseResource<CollectionCommit, IdMixin, CollectionIdMixin, MappingsMixin, StatusMixin, CreatedOnMixin>;
    CollectionCommit(ID_TYPE id, ID_TYPE collection_id, const MappingT& mappings = {}, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    std::string ToString() const override;
};

using CollectionCommitPtr = std::shared_ptr<CollectionCommit>;

class Partition : public DBBaseResource<Partition,
                                        IdMixin,
                                        NameMixin,
                                        CollectionIdMixin,
                                        StatusMixin,
                                        CreatedOnMixin>
{
public:
    using BaseT = DBBaseResource<Partition, IdMixin, NameMixin, CollectionIdMixin, StatusMixin, CreatedOnMixin>;

    Partition(ID_TYPE id, const std::string& name, ID_TYPE collection_id, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    std::string ToString() const override;
};

using PartitionPtr = std::shared_ptr<Partition>;

class PartitionCommit : public DBBaseResource<PartitionCommit,
                                              IdMixin,
                                              CollectionIdMixin,
                                              PartitionIdMixin,
                                              MappingsMixin,
                                              StatusMixin,
                                              CreatedOnMixin>
{
public:
    using BaseT = DBBaseResource<PartitionCommit, IdMixin, CollectionIdMixin, PartitionIdMixin, MappingsMixin, StatusMixin, CreatedOnMixin>;
    PartitionCommit(ID_TYPE id, ID_TYPE collection_id, ID_TYPE partition_id,
            const MappingT& mappings = {}, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    std::string ToString() const override;
};

using PartitionCommitPtr = std::shared_ptr<PartitionCommit>;

#include "Resources.inl"
