#pragma once
#include "Types.h"
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


class Snapshot : public ReferenceProxy {
public:
    using Ptr = std::shared_ptr<Snapshot>;
    Snapshot(ID_TYPE id);

    ID_TYPE GetID() const { return collection_commit_->GetID();}
    ID_TYPE GetCollectionId() const { return collection_->GetID(); }
    const std::string& GetName() const { return collection_->GetName(); }
    CollectionCommitPtr GetCollectionCommit() { return collection_commit_.Get(); }
    std::vector<std::string> GetPartitionNames() const {
        std::vector<std::string> names;
        for (auto& kv : partitions_) {
            std::cout << "Partition: " << kv.second->GetName() << std::endl;
            names.push_back(kv.second->GetName());
        }
        return names;
    }

    ID_TYPE GetLatestSchemaCommitId() const {
        return latest_schema_commit_id_;
    }

    PartitionPtr GetPartition(ID_TYPE partition_id) {
        auto it = partitions_.find(partition_id);
        if (it == partitions_.end()) {
            return nullptr;
        }
        return it->second.Get();
    }

    // PXU TODO: add const. Need to change Scopedxxxx::Get
    SegmentCommitPtr GetSegmentCommit(ID_TYPE segment_id) {
        auto it = seg_segc_map_.find(segment_id);
        if (it == seg_segc_map_.end()) return nullptr;
        auto itsc = segment_commits_.find(it->second);
        if (itsc == segment_commits_.end()) {
            return nullptr;
        }
        return itsc->second.Get();
    }

    PartitionCommitPtr GetPartitionCommit(ID_TYPE partition_id) {
        auto it = p_pc_map_.find(partition_id);
        if (it == p_pc_map_.end()) return nullptr;
        auto itpc = partition_commits_.find(it->second);
        if (itpc == partition_commits_.end()) {
            return nullptr;
        }
        return itpc->second.Get();
    }

    IDS_TYPE GetPartitionIds() const {
        IDS_TYPE ids;
        for(auto& kv : partitions_) {
            ids.push_back(kv.first);
        }
        return std::move(ids);
    }

    std::vector<std::string> GetFieldNames() const {
        std::vector<std::string> names;
        for(auto& kv : field_names_map_) {
            names.emplace_back(kv.first);
        }
        return std::move(names);
    }

    bool HasField(const std::string& name) const {
        auto it = field_names_map_.find(name);
        return it != field_names_map_.end();
    }

    bool HasFieldElement(const std::string& field_name, const std::string& field_element_name) const {
        auto id = GetFieldElementId(field_name, field_element_name);
        return id > 0;
    }

    ID_TYPE GetSegmentFileId(const std::string& field_name, const std::string& field_element_name,
            ID_TYPE segment_id) const {
        auto field_element_id = GetFieldElementId(field_name, field_element_name);
        auto it = element_segfiles_map_.find(field_element_id);
        if (it == element_segfiles_map_.end()) {
            return 0;
        }
        auto its = it->second.find(segment_id);
        if (its == it->second.end()) {
            return 0;
        }
        return its->second;
    }

    bool HasSegmentFile(const std::string& field_name, const std::string& field_element_name,
            ID_TYPE segment_id) const {
        auto id = GetSegmentFileId(field_name, field_element_name, segment_id);
        return id > 0;
    }

    ID_TYPE GetFieldElementId(const std::string& field_name, const std::string& field_element_name) const {
        auto itf = field_element_names_map_.find(field_name);
        if (itf == field_element_names_map_.end()) return false;
        auto itfe = itf->second.find(field_element_name);
        if (itfe == itf->second.end()) {
            return 0;
        }

        return itfe->second;
    }

    std::vector<std::string> GetFieldElementNames() const {
        std::vector<std::string> names;
        for(auto& kv : field_elements_) {
            names.emplace_back(kv.second->GetName());
        }

        return std::move(names);
    }

    IDS_TYPE GetSegmentIds() const {
        IDS_TYPE ids;
        for(auto& kv : segments_) {
            ids.push_back(kv.first);
        }
        return std::move(ids);
    }

    IDS_TYPE GetSegmentFileIds() const {
        IDS_TYPE ids;
        for(auto& kv : segment_files_) {
            ids.push_back(kv.first);
        }
        return std::move(ids);
    }

    void RefAll();
    void UnRefAll();

private:
    // PXU TODO: Re-org below data structures to reduce memory usage
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
    std::map<std::string, ID_TYPE> field_names_map_;
    std::map<std::string, std::map<std::string, ID_TYPE>> field_element_names_map_;
    std::map<ID_TYPE, std::map<ID_TYPE, ID_TYPE>> element_segfiles_map_;
    std::map<ID_TYPE, ID_TYPE> seg_segc_map_;
    std::map<ID_TYPE, ID_TYPE> p_pc_map_;
    ID_TYPE latest_schema_commit_id_;
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
    /* std::cout << "UnRefAll YYYYYYYYYYYY " << collection_->GetID() << " RefCnt=" << RefCnt() << std::endl; */
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
        p_pc_map_[partition_commit->GetPartitionId()] = partition_commit->GetID();
        partitions_[partition_commit->GetPartitionId()] = partition;
        auto& s_c_mappings = partition_commit->GetMappings();
        for (auto& s_c_id : s_c_mappings) {
            auto segment_commit = segment_commits_holder.GetResource(s_c_id, false);
            auto segment = segments_holder.GetResource(segment_commit->GetSegmentId(), false);
            auto schema = schema_holder.GetResource(segment_commit->GetSchemaId(), false);
            schema_commits_[schema->GetID()] = schema;
            segment_commits_[segment_commit->GetID()] = segment_commit;
            segments_[segment->GetID()] = segment;
            seg_segc_map_[segment->GetID()] = segment_commit->GetID();
            auto& s_f_mappings = segment_commit->GetMappings();
            for (auto& s_f_id : s_f_mappings) {
                auto segment_file = segment_files_holder.GetResource(s_f_id, false);
                auto field_element = field_elements_holder.GetResource(segment_file->GetFieldElementId(), false);
                field_elements_[field_element->GetID()] = field_element;
                segment_files_[s_f_id] = segment_file;
                auto entry = element_segfiles_map_.find(segment_file->GetFieldElementId());
                if (entry == element_segfiles_map_.end()) {
                    element_segfiles_map_[segment_file->GetFieldElementId()] = {
                        {segment_file->GetSegmentId(), segment_file->GetID()}
                    };
                } else {
                    entry->second[segment_file->GetSegmentId()] = segment_file->GetID();
                }
            }
        }
    }

    for (auto& kv : schema_commits_) {
        if (kv.first > latest_schema_commit_id_) latest_schema_commit_id_ = kv.first;
        auto& schema_commit = kv.second;
        auto& s_c_m =  current_schema->GetMappings();
        for (auto field_commit_id : s_c_m) {
            auto field_commit = field_commits_holder.GetResource(field_commit_id, false);
            field_commits_[field_commit_id] = field_commit;
            auto field = fields_holder.GetResource(field_commit->GetFieldId(), false);
            fields_[field->GetID()] = field;
            field_names_map_[field->GetName()] = field->GetID();
            auto& f_c_m = field_commit->GetMappings();
            for (auto field_element_id : f_c_m) {
                auto field_element = field_elements_holder.GetResource(field_element_id, false);
                field_elements_[field_element_id] = field_element;
                auto entry = field_element_names_map_.find(field->GetName());
                if (entry == field_element_names_map_.end()) {
                    field_element_names_map_[field->GetName()] = {{field_element->GetName(), field_element->GetID()}};
                } else {
                    entry->second[field_element->GetName()] = field_element->GetID();
                }
            }
        }
    }

    RefAll();
};

using ScopedSnapshotT = ScopedResource<Snapshot>;
using GCHandler = std::function<void(Snapshot::Ptr)>;
