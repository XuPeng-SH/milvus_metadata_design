#pragma once
#include "Resources.h"
#include "Holders.h"
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
#include <shared_mutex>
#include <thread>
#include <atomic>
#include <chrono>


using CollectionScopedT = ScopedResource<Collection>;
using CollectionCommitScopedT = ScopedResource<CollectionCommit>;

using SchemaCommitScopedT = ScopedResource<SchemaCommit>;
using SchemaCommitsT = std::map<ID_TYPE, SchemaCommitScopedT>;

using FieldScopedT = ScopedResource<Field>;
using FieldsT = std::map<ID_TYPE, FieldScopedT>;
using FieldCommitScopedT = ScopedResource<FieldCommit>;
using FieldCommitsT = std::map<ID_TYPE, FieldCommitScopedT>;
using FieldElementScopedT = ScopedResource<FieldElement>;
using FieldElementsT = std::map<ID_TYPE, FieldElementScopedT>;

using PartitionScopedT = ScopedResource<Partition>;
using PartitionCommitScopedT = ScopedResource<PartitionCommit>;
using PartitionsT = std::map<ID_TYPE, PartitionScopedT>;
using PartitionCommitsT = std::map<ID_TYPE, PartitionCommitScopedT>;

using SegmentScopedT = ScopedResource<Segment>;
using SegmentCommitScopedT = ScopedResource<SegmentCommit>;
using SegmentFileScopedT = ScopedResource<SegmentFile>;
using SegmentsT = std::map<ID_TYPE, SegmentScopedT>;
using SegmentCommitsT = std::map<ID_TYPE, SegmentCommitScopedT>;
using SegmentFilesT = std::map<ID_TYPE, SegmentFileScopedT>;

class Snapshot : public ReferenceProxy {
public:
    using Ptr = std::shared_ptr<Snapshot>;
    Snapshot(ID_TYPE id);

    ID_TYPE GetID() const { return collection_commit_->GetID();}
    const std::string& GetName() const { return collection_->GetName(); }

    void RefAll();
    void UnRefAll();

private:

    CollectionScopedT collection_;
    ID_TYPE current_schema_id_;
    SchemaCommitsT schema_commits_;
    FieldsT fields_;
    FieldCommitsT field_commits_;
    FieldElementsT field_elements_;
    CollectionCommitScopedT collection_commit_;
    PartitionsT partitions_;
    PartitionCommitsT partition_commits_;
    SegmentsT segments_;
    SegmentCommitsT segment_commits_;
    SegmentFilesT segment_files_;
};

void Snapshot::RefAll() {
    collection_commit_->Ref();
    for (auto& schema : schema_commits_) {
        schema.second->Ref();
    }
    for (auto& element : field_elements_) {
        element.second->Ref();
    }
    for (auto& field : fields_) {
        field.second->Ref();
    }
    for (auto& field_commit : field_commits_) {
        field_commit.second->Ref();
    }
    collection_->Ref();
    for (auto& partition : partitions_) {
        partition.second->Ref();
    }
    for (auto& partition_commit : partition_commits_) {
        partition_commit.second->Ref();
    }
    for (auto& segment : segments_) {
        segment.second->Ref();
    }
    for (auto& segment_commit : segment_commits_) {
        segment_commit.second->Ref();
    }
    for (auto& segment_file : segment_files_) {
        segment_file.second->Ref();
    }
}

void Snapshot::UnRefAll() {
    collection_commit_->UnRef();
    for (auto& schema : schema_commits_) {
        schema.second->UnRef();
    }
    for (auto& element : field_elements_) {
        element.second->UnRef();
    }
    for (auto& field : fields_) {
        field.second->UnRef();
    }
    for (auto& field_commit : field_commits_) {
        field_commit.second->UnRef();
    }
    collection_->UnRef();
    for (auto& partition : partitions_) {
        partition.second->UnRef();
    }
    for (auto& partition_commit : partition_commits_) {
        partition_commit.second->UnRef();
    }
    for (auto& segment : segments_) {
        segment.second->UnRef();
    }
    for (auto& segment_commit : segment_commits_) {
        segment_commit.second->UnRef();
    }
    for (auto& segment_file : segment_files_) {
        segment_file.second->UnRef();
    }
}

Snapshot::Snapshot(ID_TYPE id) {
    collection_commit_ = CollectionCommitsHolder::GetInstance().GetResource(id, false);
    assert(collection_commit_);
    auto& schema_holder =  SchemaCommitsHolder::GetInstance();
    auto current_schema = schema_holder.GetResource(collection_commit_->GetSchemaId(), false);
    schema_commits_[current_schema->GetID()] = current_schema;
    current_schema_id_ = current_schema->GetID();
    auto& field_commits_holder = FieldCommitsHolder::GetInstance();
    auto& fields_holder = FieldsHolder::GetInstance();
    auto& field_elements_holder = FieldElementsHolder::GetInstance();

    collection_ = CollectionsHolder::GetInstance().GetResource(collection_commit_->GetCollectionId(), false);
    auto& mappings =  collection_commit_->GetMappings();
    auto& partition_commits_holder = PartitionCommitsHolder::GetInstance();
    auto& partitions_holder = PartitionsHolder::GetInstance();
    auto& segments_holder = SegmentsHolder::GetInstance();
    auto& segment_commits_holder = SegmentCommitsHolder::GetInstance();
    auto& segment_files_holder = SegmentFilesHolder::GetInstance();

    for (auto& id : mappings) {
        auto partition_commit = partition_commits_holder.GetResource(id, false);
        auto partition = partitions_holder.GetResource(partition_commit->GetPartitionId(), false);
        partition_commits_[partition_commit->GetPartitionId()] = partition_commit;
        partitions_[partition_commit->GetPartitionId()] = partition;
        auto& s_c_mappings = partition_commit->GetMappings();
        for (auto& s_c_id : s_c_mappings) {
            auto segment_commit = segment_commits_holder.GetResource(s_c_id, false);
            auto segment = segments_holder.GetResource(segment_commit->GetSegmentId(), false);
            auto schema = schema_holder.GetResource(segment_commit->GetSchemaId(), false);
            schema_commits_[schema->GetID()] = schema;
            segment_commits_[segment_commit->GetID()] = segment_commit;
            segments_[segment->GetID()] = segment;
            auto& s_f_mappings = segment_commit->GetMappings();
            for (auto& s_f_id : s_f_mappings) {
                auto segment_file = segment_files_holder.GetResource(s_f_id, false);
                auto field_element = field_elements_holder.GetResource(segment_file->GetFieldElementId(), false);
                field_elements_[field_element->GetID()] = field_element;
                segment_files_[s_f_id] = segment_file;
            }
        }
    }

    for (auto& kv : schema_commits_) {
        auto& schema_commit = kv.second;
        auto& s_c_m =  current_schema->GetMappings();
        for (auto field_commit_id : s_c_m) {
            auto field_commit = field_commits_holder.GetResource(field_commit_id, false);
            field_commits_[field_commit_id] = field_commit;
            auto field = fields_holder.GetResource(field_commit->GetFieldId(), false);
            fields_[field->GetID()] = field;
            auto& f_c_m = field_commit->GetMappings();
            for (auto field_element_id : f_c_m) {
                auto field_element = field_elements_holder.GetResource(field_element_id, false);
                field_elements_[field_element_id] = field_element;
            }
        }
    }

    RefAll();
};

using GCHandler = std::function<void(Snapshot::Ptr)>;

class SnapshotsHolder {
public:
    using ScopedSnapshotT = ScopedResource<Snapshot>;
    using ScopedPtr = std::shared_ptr<ScopedSnapshotT>;

    SnapshotsHolder(ID_TYPE collection_id, GCHandler gc_handler = nullptr, size_t num_versions = 1)
        : collection_id_(collection_id), num_versions_(num_versions), gc_handler_(gc_handler), done_(false) {}

    /* SnapshotsHolder(ID_TYPE collection_id, size_t num_versions = 1) */
    /*     : collection_id_(collection_id), num_versions_(num_versions), done_(false) {} */

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

SnapshotsHolder::ScopedSnapshotT
SnapshotsHolder::GetSnapshot(ID_TYPE id, bool scoped) {
    std::unique_lock<std::mutex> lock(mutex_);
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

    IDS_TYPE GetCollectionIds() const;

private:
    void SnapshotGCCallback(Snapshot::Ptr ss_ptr);
    Snapshots() {
        Init();
    }
    void Init();

    mutable std::shared_mutex mutex_;
    SnapshotsHolderPtr LoadNoLock(ID_TYPE collection_id);
    SnapshotsHolderPtr Load(ID_TYPE collection_id);
    SnapshotsHolderPtr GetHolderNoLock(ID_TYPE collection_id);

    std::map<ID_TYPE, SnapshotsHolderPtr> holders_;
    std::map<std::string, ID_TYPE> name_id_map_;
    std::vector<Snapshot::Ptr> to_release_;
};

IDS_TYPE
Snapshots::GetCollectionIds() const {
    IDS_TYPE ids;
    std::shared_lock lock(mutex_);
    for(auto& kv : holders_) {
        ids.push_back(kv.first);
    }
    return ids;
}

bool
Snapshots::Close(ID_TYPE collection_id) {
    auto name = GetHolder(collection_id)->GetSnapshot()->GetName();
    std::unique_lock lock(mutex_);
    holders_.erase(collection_id);
    name_id_map_.erase(name);
    return true;
}

SnapshotsHolderPtr
Snapshots::Load(ID_TYPE collection_id) {
    std::unique_lock lock(mutex_);
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
    std::unique_lock lock(mutex_);
    auto kv = name_id_map_.find(name);
    if (kv != name_id_map_.end()) {
        return GetHolderNoLock(kv->second);
    }
    lock.release();
    auto& store = Store::GetInstance();
    auto c = store.GetCollection(name);
    if (!c) return nullptr;
    return Load(c->GetID());
}

SnapshotsHolderPtr
Snapshots::GetHolder(ID_TYPE collection_id) {
    std::unique_lock lock(mutex_);
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
