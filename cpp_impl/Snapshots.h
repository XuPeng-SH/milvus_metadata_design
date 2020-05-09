#pragma once
#include "Resources.h"
#include <map>
#include <memory>
#include <deque>
#include <string>
#include <vector>
#include <assert.h>
#include <iostream>
#include <limits>
#include <cstddef>
#include <mutex>
#include <thread>
#include <atomic>
#include <chrono>


using Strings = std::vector<std::string>;
using Collections = std::vector<Collection>;

using CollectionScopedT = ScopedResource<Collection>;
using CollectionCommitScopedT = ScopedResource<CollectionCommit>;

class Snapshot : public ReferenceProxy {
public:
    using Ptr = std::shared_ptr<Snapshot>;
    Snapshot(ID_TYPE id);

    ID_TYPE GetID() const { return collection_commit_->GetID();}

    void UnRefAll();
private:

    /* PartitionCommits */
    /* Partitions */
    /* Schema::Ptr */
    /* FieldCommits */
    /* Fields */
    /* FieldElements */
    /* SegmentCommits */
    /* Segments */
    /* SegmentFiles */

    CollectionScopedT collection_;
    CollectionCommitScopedT collection_commit_;
};

void Snapshot::UnRefAll() {
    collection_commit_->UnRef();
    collection_->UnRef();
    /* auto c = collection_->Get(); */
    /* std::cout << "XXXXXXXXXXXXXXXXX Collection " << c->GetID() << " RefCnt=" << c->RefCnt()  << std::endl; */
}

Snapshot::Snapshot(ID_TYPE id) {
    collection_commit_ = CollectionCommitsHolder::GetInstance().GetResource(id, false);
    assert(collection_commit_);
    collection_ = CollectionsHolder::GetInstance().GetResource(collection_commit_->GetCollectionId(), false);
    collection_commit_->Ref();
    collection_->Ref();
    /* std::cout << "c_c refcnt=" <<  collection_commit_->Get()->RefCnt() << std::endl; */
    /* auto& mappings =  collection_commit_->GetMappings(); */
    /* auto& partition_commits_holder = PartitionCommitsHolder::GetInstance(); */
    /* auto& partitions_holder = PartitionsHolder::GetInstance(); */
    /* for (auto& id : mappings) { */
    /*     partition_commit = partition_commits_holder.GetResource(id); */
    /*     partition = partitions_holder.GetResource(partition_commit->GetPartitionID()); */
    /*     partition_commits_[partition_commit->GetPartitionID()] = partition_commit; */
    /*     partitions_[partition_commit->GetPartitionID()] = partition; */
    /*     auto& s_c_mappings = partition_commit->GetMappings(); */
    /*     for (auto& s_c_id : s_c_mappings) { */
    /*         segment_commit = segment_commits_holder.GetResource(s_c_id); */
    /*         segment = segments_holder.GetResource(segment_commit->GetSegmentID()); */
    /*         segment_commits_[segment_commit->GetSegmentID()] = segment_commit; */
    /*         segments_[segment_commit->GetGetSegmentID()] = segment; */
    /*         auto& s_f_mappings = segment_commit->GetMappings(); */
    /*         for (auto& s_f_id : s_f_mappings) { */
    /*             segment_file = segment_files_holder.GetResource(s_f_id); */
    /*             segment_files_[segment_commit->GetSegmentID()][s_f_id] = segment_file; */
    /*         } */
    /*     } */
    /* } */
    /* schema_commit = SchemaCommitsHolder::GetInstance().GetResource(collection_commit_->GetSchemaCommitId()); */
    /* auto& f_c_mappings =  schema_commit->GetMappings(); */
    /* for (auto& f_c_id : f_c_mappings) { */
    /*     field_commit = field_commits_holder.GetResource(f_c_id); */
    /*     field = fields_holder.GetResource(field_commit->GetFieldID()); */
    /*     field_commits_[field_commit->GetFieldID()] = field_commit; */
    /*     fields_[fields_commit->GetFieldID()] = field; */
    /*     auto& f_e_mappings = field_commit->GetMappings(); */
    /*     for (auto& f_e_id : f_e_mappings) { */
    /*         field_element = field_elements_holder.GetResource(f_e_id); */
    /*         field_elements_[field_commit->GetFieldID()][f_e_id] = field_element; */
    /*     } */
    /* } */
};


class SnapshotsHolder {
public:
    using ScopedSnapshotT = ScopedResource<Snapshot>;
    using ScopedPtr = std::shared_ptr<ScopedSnapshotT>;

    SnapshotsHolder(size_t num_versions = 1) : num_versions_(num_versions), done_(false) {}
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

private:
    void ReadyForRelease(Snapshot::Ptr ss) {
        std::unique_lock<std::mutex> lock(gcmutex_);
        to_release_.push_back(ss);
        lock.unlock();
        cv_.notify_one();
    }

    std::mutex mutex_;
    std::mutex gcmutex_;
    std::condition_variable cv_;
    ID_TYPE min_id_ = std::numeric_limits<ID_TYPE>::max();
    ID_TYPE max_id_ = std::numeric_limits<ID_TYPE>::min();
    std::map<ID_TYPE, Snapshot::Ptr> active_;
    std::vector<Snapshot::Ptr> to_release_;
    size_t num_versions_ = 1;
    std::atomic<bool> done_;
};

SnapshotsHolder::ScopedSnapshotT
SnapshotsHolder::GetSnapshot(ID_TYPE id, bool scoped) {
    std::unique_lock<std::mutex> lock(mutex_);
    if (id == 0 || id == max_id_) {
        auto ss = active_[id];
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
                std::cout << "size = " << to_release_.size() << std::endl;
                sss = to_release_;
                to_release_.clear();
            }
        }

        for (auto& ss : sss) {
            ss->UnRef();
            std::cout << "BG Handling " << ss->GetID() << " RefCnt=" << ss->RefCnt() << std::endl;
        }

    }
}
