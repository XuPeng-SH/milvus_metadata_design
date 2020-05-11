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
    ResourcePtr Load(ID_TYPE id) override;
    ResourcePtr Load(const std::string& name) override;
    bool HardDelete(ID_TYPE id) override;

    NameMapT name_map_;
};

class SchemaCommitsHolder : public ResourceHolder<SchemaCommit, SchemaCommitsHolder> {
public:
    using BaseT = ResourceHolder<SchemaCommit, SchemaCommitsHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;

private:
    ResourcePtr Load(ID_TYPE id) override;
    bool HardDelete(ID_TYPE id) override;
};

class CollectionCommitsHolder : public ResourceHolder<CollectionCommit, CollectionCommitsHolder> {
public:
    using BaseT = ResourceHolder<CollectionCommit, CollectionCommitsHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;

private:
    ResourcePtr Load(ID_TYPE id) override;
    bool HardDelete(ID_TYPE id) override;
};

class PartitionsHolder : public ResourceHolder<Partition, PartitionsHolder> {
public:
    using BaseT = ResourceHolder<Partition, PartitionsHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;

private:
    ResourcePtr Load(ID_TYPE id) override;
    bool HardDelete(ID_TYPE id) override;
};

class PartitionCommitsHolder : public ResourceHolder<PartitionCommit, PartitionCommitsHolder> {
public:
    using BaseT = ResourceHolder<PartitionCommit, PartitionCommitsHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;

private:
    ResourcePtr Load(ID_TYPE id) override;
    bool HardDelete(ID_TYPE id) override;
};

class SegmentsHolder : public ResourceHolder<Segment, SegmentsHolder> {
public:
    using BaseT = ResourceHolder<Segment, SegmentsHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;

private:
    ResourcePtr Load(ID_TYPE id) override;
    bool HardDelete(ID_TYPE id) override;
};

class SegmentCommitsHolder : public ResourceHolder<SegmentCommit, SegmentCommitsHolder> {
public:
    using BaseT = ResourceHolder<SegmentCommit, SegmentCommitsHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;

private:
    ResourcePtr Load(ID_TYPE id) override;
    bool HardDelete(ID_TYPE id) override;
};

class SegmentFilesHolder : public ResourceHolder<SegmentFile, SegmentFilesHolder> {
public:
    using BaseT = ResourceHolder<SegmentFile, SegmentFilesHolder>;
    using ResourcePtr = typename BaseT::ResourcePtr;

private:
    ResourcePtr Load(ID_TYPE id) override;
    bool HardDelete(ID_TYPE id) override;
};

#include "Holders.inl"
