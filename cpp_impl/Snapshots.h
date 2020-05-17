#pragma once
#include "Snapshot.h"
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


class SnapshotsHolder {
public:
    using ScopedPtr = std::shared_ptr<ScopedSnapshotT>;

    SnapshotsHolder(ID_TYPE collection_id, GCHandler gc_handler = nullptr, size_t num_versions = 1)
        : collection_id_(collection_id), num_versions_(num_versions), gc_handler_(gc_handler), done_(false) {}

    ID_TYPE GetID() const { return collection_id_; }
    bool Add(ID_TYPE id) {
        {
            std::unique_lock<std::mutex> lock(mutex_);
            if (active_.size() > 0 && id < max_id_) {
                return false;
            }
        }
        Snapshot::Ptr oldest_ss;
        {
            auto ss = std::make_shared<Snapshot>(id);

            std::unique_lock<std::mutex> lock(mutex_);
            if (done_) { return false; };
            ss->RegisterOnNoRefCB(std::bind(&Snapshot::UnRefAll, ss));
            ss->Ref();
            auto it = active_.find(id);
            if (it != active_.end()) {
                return false;
            }

            if (min_id_ > id) {
                min_id_ = id;
            }

            if (max_id_ < id) {
                max_id_ = id;
            }

            active_[id] = ss;
            if (active_.size() <= num_versions_)
                return true;

            auto oldest_it = active_.find(min_id_);
            oldest_ss = oldest_it->second;
            active_.erase(oldest_it);
            min_id_ = active_.begin()->first;
        }
        ReadyForRelease(oldest_ss); // TODO: Use different mutex
        return true;
    }

    void BackgroundGC();

    void NotifyDone();

    ScopedSnapshotT GetSnapshot(ID_TYPE id = 0, bool scoped = true);

    void GCHandlerTestCallBack(Snapshot::Ptr ss) {
        std::unique_lock<std::mutex> lock(gcmutex_);
        to_release_.push_back(ss);
        lock.unlock();
        cv_.notify_one();
    }

    bool SetGCHandler(GCHandler gc_handler) {
        gc_handler_ = gc_handler;
    }

private:
    void ReadyForRelease(Snapshot::Ptr ss) {
        if (gc_handler_) {
            gc_handler_(ss);
        }
    }

    std::mutex mutex_;
    std::mutex gcmutex_;
    std::condition_variable cv_;
    ID_TYPE collection_id_;
    ID_TYPE min_id_ = std::numeric_limits<ID_TYPE>::max();
    ID_TYPE max_id_ = std::numeric_limits<ID_TYPE>::min();
    std::map<ID_TYPE, Snapshot::Ptr> active_;
    std::vector<Snapshot::Ptr> to_release_;
    size_t num_versions_ = 1;
    GCHandler gc_handler_;
    std::atomic<bool> done_;
};

ScopedSnapshotT
SnapshotsHolder::GetSnapshot(ID_TYPE id, bool scoped) {
    std::unique_lock<std::mutex> lock(mutex_);
    /* std::cout << "Holder " << collection_id_ << " actives num=" << active_.size() */
    /*     << " latest=" << active_[max_id_]->GetID() << " RefCnt=" << active_[max_id_]->RefCnt() <<  std::endl; */
    if (id == 0 || id == max_id_) {
        auto ss = active_[max_id_];
        return ScopedSnapshotT(ss, scoped);
    }
    if (id < min_id_ || id > max_id_) {
        return ScopedSnapshotT();
    }

    auto it = active_.find(id);
    if (it == active_.end()) {
        return ScopedSnapshotT();
    }
    return ScopedSnapshotT(it->second, scoped);
}

void
SnapshotsHolder::NotifyDone() {
    std::unique_lock<std::mutex> lock(gcmutex_);
    done_ = true;
    cv_.notify_all();
}

void
SnapshotsHolder::BackgroundGC() {
    while (true) {
        if (done_.load(std::memory_order_acquire)) {
            break;
        }
        std::vector<Snapshot::Ptr> sss;
        {
            std::unique_lock<std::mutex> lock(gcmutex_);
            cv_.wait_for(lock, std::chrono::milliseconds(100), [this]() {return to_release_.size() > 0;});
            if (to_release_.size() > 0) {
                /* std::cout << "size = " << to_release_.size() << std::endl; */
                sss = to_release_;
                to_release_.clear();
            }
        }

        for (auto& ss : sss) {
            ss->UnRef();
            /* std::cout << "BG Handling " << ss->GetID() << " RefCnt=" << ss->RefCnt() << std::endl; */
        }

    }
}

using SnapshotsHolderPtr = std::shared_ptr<SnapshotsHolder>;

class Snapshots {
public:
    static Snapshots& GetInstance() {
        static Snapshots sss;
        return sss;
    }
    bool Close(ID_TYPE collection_id);
    SnapshotsHolderPtr GetHolder(ID_TYPE collection_id);
    SnapshotsHolderPtr GetHolder(const std::string& name);

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
    SnapshotsHolderPtr LoadNoLock(ID_TYPE collection_id);
    SnapshotsHolderPtr Load(ID_TYPE collection_id);
    SnapshotsHolderPtr GetHolderNoLock(ID_TYPE collection_id);

    std::map<ID_TYPE, SnapshotsHolderPtr> holders_;
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

SnapshotsHolderPtr
Snapshots::Load(ID_TYPE collection_id) {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    return LoadNoLock(collection_id);
}

SnapshotsHolderPtr
Snapshots::LoadNoLock(ID_TYPE collection_id) {
    auto& store = Store::GetInstance();
    auto collection_commit_ids = store.AllActiveCollectionCommitIds(collection_id, false);
    if (collection_commit_ids.size() == 0) {
        return nullptr;
    }
    auto holder = std::make_shared<SnapshotsHolder>(collection_id,
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

SnapshotsHolderPtr
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

SnapshotsHolderPtr
Snapshots::GetHolder(ID_TYPE collection_id) {
    std::unique_lock<std::shared_timed_mutex> lock(mutex_);
    return GetHolderNoLock(collection_id);
}

SnapshotsHolderPtr
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
