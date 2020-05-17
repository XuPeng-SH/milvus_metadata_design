#include "BaseHolders.h"
#include "Store.h"
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
    auto& store = Store::GetInstance();
    auto c = store.GetResource<ResourceT>(id);
    if (c) {
        AddNoLock(c);
        return c;
    }
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
    auto& store = Store::GetInstance();
    bool ok = store.RemoveResource<ResourceT>(id);
    return ok;
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
