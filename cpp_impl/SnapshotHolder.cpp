#include "SnapshotHolder.h"

SnapshotHolder::SnapshotHolder(ID_TYPE collection_id, GCHandler gc_handler, size_t num_versions)
    : collection_id_(collection_id),
      num_versions_(num_versions),
      gc_handler_(gc_handler),
      done_(false) {
}

ScopedSnapshotT
SnapshotHolder::GetSnapshot(ID_TYPE id, bool scoped) {
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
SnapshotHolder::NotifyDone() {
    std::unique_lock<std::mutex> lock(gcmutex_);
    done_ = true;
    cv_.notify_all();
}

void
SnapshotHolder::BackgroundGC() {
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
