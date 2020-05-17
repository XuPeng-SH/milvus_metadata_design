#pragma once
#include "SnapshotHolder.h"
#include "Resources.h"
#include "Holders.h"
#include "Store.h"
#include "schema.pb.h"
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <assert.h>
#include <iostream>
#include <limits>
#include <cstddef>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>


class Snapshots {
public:
    static Snapshots& GetInstance() {
        static Snapshots sss;
        return sss;
    }
    bool Close(ID_TYPE collection_id);
    SnapshotHolderPtr GetHolder(ID_TYPE collection_id);
    SnapshotHolderPtr GetHolder(const std::string& name);

    ScopedSnapshotT GetSnapshot(ID_TYPE collection_id, ID_TYPE id = 0, bool scoped = true);
    ScopedSnapshotT GetSnapshot(const std::string& name, ID_TYPE id = 0, bool scoped = true);

    IDS_TYPE GetCollectionIds() const;

    bool DropCollection(const std::string& name);

    template<typename ...ResourceT>
    bool Flush(ResourceT&&... resources);

private:
    void SnapshotGCCallback(Snapshot::Ptr ss_ptr);
    Snapshots() {
        Init();
    }
    void Init();

    mutable std::shared_timed_mutex mutex_;
    SnapshotHolderPtr LoadNoLock(ID_TYPE collection_id);
    SnapshotHolderPtr Load(ID_TYPE collection_id);
    SnapshotHolderPtr GetHolderNoLock(ID_TYPE collection_id);

    std::map<ID_TYPE, SnapshotHolderPtr> holders_;
    std::map<std::string, ID_TYPE> name_id_map_;
    std::vector<Snapshot::Ptr> to_release_;
};

template<typename ...ResourceT>
bool Snapshots::Flush(ResourceT&&... resources) {
    auto t = std::make_tuple(resources...);
    std::apply([](auto&&... args) {((std::cout << args << "\n"), ...);}, t);
    return true;
}

bool
Snapshots::DropCollection(const std::string& name) {
    std::map<std::string, ID_TYPE>::iterator it;
    {
        std::shared_lock<std::shared_timed_mutex> lock(mutex_);
        it = name_id_map_.find(name);
        if (it == name_id_map_.end()) {
            return false;
        }
    }

    return true;
}

ScopedSnapshotT
Snapshots::GetSnapshot(ID_TYPE collection_id, ID_TYPE id, bool scoped) {
    auto holder = GetHolder(collection_id);
    if (!holder) return ScopedSnapshotT();
    return holder->GetSnapshot(id, scoped);
}

ScopedSnapshotT
Snapshots::GetSnapshot(const std::string& name, ID_TYPE id, bool scoped) {
    auto holder = GetHolder(name);
    if (!holder) return ScopedSnapshotT();
    return holder->GetSnapshot(id, scoped);
}

IDS_TYPE
Snapshots::GetCollectionIds() const {
    IDS_TYPE ids;
    std::shared_lock<std::shared_timed_mutex> lock(mutex_);
    for(auto& kv : holders_) {
        ids.push_back(kv.first);
    }
    return ids;
}

bool
Snapshots::Close(ID_TYPE collection_id) {
    auto ss = GetSnapshot(collection_id);
    if (!ss) return false;
    auto name = ss->GetName();
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    holders_.erase(collection_id);
    name_id_map_.erase(name);
    return true;
}

SnapshotHolderPtr
Snapshots::Load(ID_TYPE collection_id) {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    return LoadNoLock(collection_id);
}

SnapshotHolderPtr
Snapshots::LoadNoLock(ID_TYPE collection_id) {
    auto& store = Store::GetInstance();
    auto collection_commit_ids = store.AllActiveCollectionCommitIds(collection_id, false);
    if (collection_commit_ids.size() == 0) {
        return nullptr;
    }
    auto holder = std::make_shared<SnapshotHolder>(collection_id,
            std::bind(&Snapshots::SnapshotGCCallback, this, std::placeholders::_1));
    for (auto c_c_id : collection_commit_ids) {
        holder->Add(c_c_id);
    }
    holders_[collection_id] = holder;
    name_id_map_[holder->GetSnapshot()->GetName()] = collection_id;
    return holder;
}

void
Snapshots::Init() {
    auto& store = Store::GetInstance();
    auto collection_ids = store.AllActiveCollectionIds();
    for (auto collection_id : collection_ids) {
        Load(collection_id);
    }
}

SnapshotHolderPtr
Snapshots::GetHolder(const std::string& name) {
    {
        std::unique_lock<std::shared_timed_mutex> lock(mutex_);
        auto kv = name_id_map_.find(name);
        if (kv != name_id_map_.end()) {
            return GetHolderNoLock(kv->second);
        }
    }
    auto& store = Store::GetInstance();
    auto c = store.GetCollection(name);
    if (!c) return nullptr;
    return Load(c->GetID());
}

SnapshotHolderPtr
Snapshots::GetHolder(ID_TYPE collection_id) {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    return GetHolderNoLock(collection_id);
}

SnapshotHolderPtr
Snapshots::GetHolderNoLock(ID_TYPE collection_id) {
    auto it = holders_.find(collection_id);
    if (it == holders_.end()) {
        return LoadNoLock(collection_id);
    }
    return it->second;
}

void
Snapshots::SnapshotGCCallback(Snapshot::Ptr ss_ptr) {
    to_release_.push_back(ss_ptr);
    std::cout << "Snapshot " << ss_ptr->GetID() << " To be removed" << std::endl;
}
