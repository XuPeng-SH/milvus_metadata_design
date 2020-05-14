#include "Holders.h"
#include "Store.h"
#include <sstream>
#include <iostream>


template <typename ResourceT, typename Derived>
void ResourceHolder<ResourceT, Derived>::Dump(const std::string& tag) {
    std::unique_lock<std::mutex> lock(mutex_);
    std::cout << typeid(*this).name() << " Dump Start [" << tag <<  "]:" << id_map_.size() << std::endl;
    for (auto& kv : id_map_) {
        std::cout << "\t" << kv.second->ToString() << std::endl;
    }
    std::cout << typeid(*this).name() << " Dump   End [" << tag <<  "]" << std::endl;
}

template <typename ResourceT, typename Derived>
typename ResourceHolder<ResourceT, Derived>::ResourcePtr
ResourceHolder<ResourceT, Derived>::Load(ID_TYPE id) {
    return nullptr;
}

template <typename ResourceT, typename Derived>
typename ResourceHolder<ResourceT, Derived>::ResourcePtr
ResourceHolder<ResourceT, Derived>::Load(const std::string& name) {
    return nullptr;
}

template <typename ResourceT, typename Derived>
typename ResourceHolder<ResourceT, Derived>::ScopedT
ResourceHolder<ResourceT, Derived>::GetResource(ID_TYPE id, bool scoped) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto cit = id_map_.find(id);
    if (cit == id_map_.end()) {
        auto ret = Load(id);
        if (!ret) return ScopedT();
        return ScopedT(ret, scoped);
    }
    return ScopedT(cit->second, scoped);
}

template <typename ResourceT, typename Derived>
void
ResourceHolder<ResourceT, Derived>::OnNoRefCallBack(typename ResourceHolder<ResourceT, Derived>::ResourcePtr resource) {
    HardDelete(resource->GetID());
    Release(resource->GetID());
}

template <typename ResourceT, typename Derived>
bool ResourceHolder<ResourceT, Derived>::ReleaseNoLock(ID_TYPE id) {
    auto it = id_map_.find(id);
    if (it == id_map_.end()) {
        return false;
    }

    id_map_.erase(it);
    return true;
}

template <typename ResourceT, typename Derived>
bool ResourceHolder<ResourceT, Derived>::Release(ID_TYPE id) {
    std::unique_lock<std::mutex> lock(mutex_);
    return ReleaseNoLock(id);
}

template <typename ResourceT, typename Derived>
bool
ResourceHolder<ResourceT, Derived>::HardDelete(ID_TYPE id) {
    return false;
}

template <typename ResourceT, typename Derived>
bool ResourceHolder<ResourceT, Derived>::AddNoLock(typename ResourceHolder<ResourceT, Derived>::ResourcePtr resource) {
    if (!resource) return false;
    if (id_map_.find(resource->GetID()) != id_map_.end()) {
        return false;
    }

    id_map_[resource->GetID()] = resource;
    resource->RegisterOnNoRefCB(std::bind(&Derived::OnNoRefCallBack, this, resource));
    return true;
}

template <typename ResourceT, typename Derived>
bool ResourceHolder<ResourceT, Derived>::Add(typename ResourceHolder<ResourceT, Derived>::ResourcePtr resource) {
    std::unique_lock<std::mutex> lock(mutex_);
    return AddNoLock(resource);
}

CollectionsHolder::ResourcePtr
CollectionsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetCollection(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
CollectionsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemoveCollection(id);
    return ok;
}

CollectionsHolder::ResourcePtr
CollectionsHolder::Load(const std::string& name) {
    auto& store = Store::GetInstance();
    auto c = store.GetCollection(name);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

CollectionsHolder::ScopedT
CollectionsHolder::GetCollection(const std::string& name, bool scoped) {
    std::unique_lock<std::mutex> lock(BaseT::mutex_);
    auto cit = name_map_.find(name);
    if (cit == name_map_.end()) {
        auto ret = Load(name);
        if (!ret) return BaseT::ScopedT();
        return BaseT::ScopedT(ret, scoped);
    }
    return BaseT::ScopedT(cit->second, scoped);
}

bool CollectionsHolder::Add(CollectionsHolder::ResourcePtr resource) {
    if (!resource) return false;
    std::unique_lock<std::mutex> lock(BaseT::mutex_);
    return BaseT::AddNoLock(resource);
}

bool CollectionsHolder::Release(const std::string& name) {
    std::unique_lock<std::mutex> lock(BaseT::mutex_);
    auto it = name_map_.find(name);
    if (it == name_map_.end()) {
        return false;
    }

    BaseT::id_map_.erase(it->second->GetID());
    name_map_.erase(it);
    return true;
}

bool CollectionsHolder::Release(ID_TYPE id) {
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = id_map_.find(id);
    if (it == id_map_.end()) {
        return false;
    }

    BaseT::id_map_.erase(it);
    name_map_.erase(it->second->GetName());
    return true;
}

SchemaCommitsHolder::ResourcePtr
SchemaCommitsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetSchemaCommit(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
SchemaCommitsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemoveSchemaCommit(id);
    return ok;
}

FieldCommitsHolder::ResourcePtr
FieldCommitsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetFieldCommit(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
FieldCommitsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemoveFieldCommit(id);
    return ok;
}

FieldsHolder::ResourcePtr
FieldsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetField(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
FieldsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemoveField(id);
    return ok;
}

FieldElementsHolder::ResourcePtr
FieldElementsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetFieldElement(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
FieldElementsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemoveFieldElement(id);
    return ok;
}

CollectionCommitsHolder::ResourcePtr
CollectionCommitsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetResource<CollectionCommit>(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
CollectionCommitsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemoveResource<CollectionCommit>(id);
    return ok;
}

PartitionsHolder::ResourcePtr
PartitionsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetPartition(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
PartitionsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemovePartition(id);
    return ok;
}

PartitionCommitsHolder::ResourcePtr
PartitionCommitsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetPartitionCommit(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
PartitionCommitsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemovePartitionCommit(id);
    return ok;
}

SegmentsHolder::ResourcePtr
SegmentsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetSegment(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
SegmentsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemoveSegment(id);
    return ok;
}

SegmentCommitsHolder::ResourcePtr
SegmentCommitsHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetSegmentCommit(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
SegmentCommitsHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemoveSegmentCommit(id);
    return ok;
}

SegmentFilesHolder::ResourcePtr
SegmentFilesHolder::Load(ID_TYPE id) {
    auto& store = Store::GetInstance();
    auto c = store.GetSegmentFile(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
    return nullptr;
}

bool
SegmentFilesHolder::HardDelete(ID_TYPE id) {
    auto& store = Store::GetInstance();
    bool ok = store.RemoveSegmentFile(id);
    return ok;
}
