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


class Collection : public DBBaseResource<Collection> {
public:
    using BaseT = DBBaseResource<Collection>;

    Collection(ID_TYPE id, const std::string& name, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    const std::string& GetName() const {return name_;}

    std::string ToString() const override;

private:
    std::string name_;
};

using CollectionPtr = std::shared_ptr<Collection>;

template <typename ResourceT, typename Derived>
class ResourceHolder {
public:
    using ResourcePtr = typename ResourceT::Ptr;
    using ScopedT = ScopedResource<ResourceT>;
    using ScopedPtr = std::shared_ptr<ScopedT>;
    using IdMapT = std::map<ID_TYPE, ResourcePtr>;
    using Ptr = std::shared_ptr<Derived>;
    ScopedT GetResource(ID_TYPE id, bool scoped = true);

    bool AddNoLock(ResourcePtr resource);
    bool ReleaseNoLock(ID_TYPE id);

    virtual bool Add(ResourcePtr resource);
    virtual bool Release(ID_TYPE id);
    virtual bool HardDelete(ID_TYPE id);

    static Derived& GetInstance() {
        static Derived holder;
        return holder;
    }

    virtual void Dump(const std::string& tag = "");

protected:
    virtual void OnNoRefCallBack(ResourcePtr resource);

    virtual ResourcePtr Load(ID_TYPE id);
    virtual ResourcePtr Load(const std::string& name);
    ResourceHolder() = default;
    virtual ~ResourceHolder() = default;

    std::mutex mutex_;
    IdMapT id_map_;
};


class CollectionsHolder : public ResourceHolder<Collection, CollectionsHolder> {
public:
    using BaseT = ResourceHolder<Collection, CollectionsHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;
    using NameMapT = std::map<std::string, ResourcePtr>;

    ScopedT GetCollection(const std::string& name, bool scoped = true);

    bool Add(ResourcePtr resource) override;
    bool Release(ID_TYPE id) override;
    bool Release(const std::string& name);

private:
    ResourcePtr Load(ID_TYPE id) override;
    ResourcePtr Load(const std::string& name) override;
    bool HardDelete(ID_TYPE id) override;

    NameMapT name_map_;
};

using CollectionsHolderPtr = std::shared_ptr<CollectionsHolder>;

class CollectionCommit : public DBBaseResource<CollectionCommit> {
public:
    using BaseT = DBBaseResource<CollectionCommit>;
    CollectionCommit(ID_TYPE id, ID_TYPE collection_id, const MappingT& mappings = {}, State status = PENDING,
            TS_TYPE created_on = GetMicroSecTimeStamp());

    const MappingT& GetMappings() const { return mappings_; }
    ID_TYPE GetCollectionId() const { return collection_id_; };

    std::string ToString() const override;

private:
    MappingT mappings_;
    ID_TYPE collection_id_;
};

using CollectionCommitPtr = std::shared_ptr<CollectionCommit>;

class CollectionCommitsHolder : public ResourceHolder<CollectionCommit, CollectionCommitsHolder> {
public:
    using BaseT = ResourceHolder<CollectionCommit, CollectionCommitsHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;

private:
    ResourcePtr Load(ID_TYPE id) override;
    bool HardDelete(ID_TYPE id) override;
};

#include "Resources.inl"
