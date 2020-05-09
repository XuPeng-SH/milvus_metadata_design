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


template <typename Derived>
class DBBaseResource : public ReferenceProxy {
public:
    using Ptr = std::shared_ptr<Derived>;
    DBBaseResource(ID_TYPE id, State status, TS_TYPE created_on);

    bool IsActive() const {return status_ == ACTIVE;}
    bool IsDeactive() const {return status_ == DEACTIVE;}

    ID_TYPE GetID() const {return id_;}
    State GetStatus() const {return status_;}
    TS_TYPE GetCreatedTime() const {return created_on_;}

    virtual std::string ToString() const;

    virtual ~DBBaseResource() {}

protected:

    ID_TYPE id_;
    State status_;
    TS_TYPE created_on_;
};

class MappingsMixin {
public:
    MappingsMixin(const MappingT& mappings) : mappings_(mappings) {}
    const MappingT& GetMappings() const { return mappings_; }

protected:
    MappingT mappings_;
};


class CollectionIdMixin {
public:
    CollectionIdMixin(ID_TYPE id) : collection_id_(id) {}
    ID_TYPE GetCollectionId() const { return collection_id_; };

protected:
    ID_TYPE collection_id_;
};


class NameMixin {
public:
    NameMixin(const std::string& name) : name_(name) {}
    const std::string& GetName() const { return name_; };

protected:
    std::string name_;
};


class Collection : public DBBaseResource<Collection>,
                   public NameMixin
{
public:
    using BaseT = DBBaseResource<Collection>;

    Collection(ID_TYPE id, const std::string& name, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    std::string ToString() const override;
};

using CollectionPtr = std::shared_ptr<Collection>;


class CollectionCommit : public DBBaseResource<CollectionCommit>,
                         public MappingsMixin,
                         public CollectionIdMixin
{
public:
    using BaseT = DBBaseResource<CollectionCommit>;
    CollectionCommit(ID_TYPE id, ID_TYPE collection_id, const MappingT& mappings = {}, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    std::string ToString() const override;
};

using CollectionCommitPtr = std::shared_ptr<CollectionCommit>;

#include "Resources.inl"
