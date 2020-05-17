#pragma once
#include "Snapshot.h"

class SnapshotHolder {
public:
    using ScopedPtr = std::shared_ptr<ScopedSnapshotT>;

    SnapshotHolder(ID_TYPE collection_id, GCHandler gc_handler = nullptr, size_t num_versions = 1);

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

using SnapshotsHolderPtr = std::shared_ptr<SnapshotHolder>;
