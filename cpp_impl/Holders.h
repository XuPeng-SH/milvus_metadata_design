#pragma once
#include "Helper.h"
#include "Schema.h"
#include "Proxy.h"
#include "Resources.h"
#include <string>
#include <map>
#include <vector>
#include <memory>
#include <condition_variable>
#include <mutex>
#include <thread>


template <typename ResourceT, typename Derived>
class ResourceHolder {
public:
    using ResourcePtr = std::shared_ptr<ResourceT>;
    /* using ResourcePtr = typename ResourceT::Ptr; */
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
    ResourcePtr Load(const std::string& name) override;

    NameMapT name_map_;
};

class SchemaCommitsHolder : public ResourceHolder<SchemaCommit, SchemaCommitsHolder> {};

class FieldCommitsHolder : public ResourceHolder<FieldCommit, FieldCommitsHolder> {};

class FieldsHolder : public ResourceHolder<Field, FieldsHolder> {};

class FieldElementsHolder : public ResourceHolder<FieldElement, FieldElementsHolder> {};

class CollectionCommitsHolder : public ResourceHolder<CollectionCommit, CollectionCommitsHolder> {};

class PartitionsHolder : public ResourceHolder<Partition, PartitionsHolder> {};

class PartitionCommitsHolder : public ResourceHolder<PartitionCommit, PartitionCommitsHolder> {};

class SegmentsHolder : public ResourceHolder<Segment, SegmentsHolder> {};

class SegmentCommitsHolder : public ResourceHolder<SegmentCommit, SegmentCommitsHolder> {};

class SegmentFilesHolder : public ResourceHolder<SegmentFile, SegmentFilesHolder> {};

#include "Holders.inl"
