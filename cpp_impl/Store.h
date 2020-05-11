#pragma once
#include "Resources.h"
#include "Schema.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>

class Store {
public:
    static Store& GetInstance() {
        static Store store;
        return store;
    }

    CollectionPtr GetCollection(ID_TYPE id) {
        auto it = id_collections_.find(id);
        if (it == id_collections_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] Collection " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<Collection>(c->GetID(), c->GetName(), c->GetStatus(), c->GetCreatedTime());
        return ret;
    }

    CollectionPtr GetCollection(const std::string& name) {
        auto it = name_collections_.find(name);
        if (it == name_collections_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] Collection " << name << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<Collection>(c->GetID(), c->GetName(), c->GetStatus(), c->GetCreatedTime());
        return ret;
    }

    bool RemoveCollection(ID_TYPE id) {
        auto it = id_collections_.find(id);
        if (it == id_collections_.end()) {
            return false;
        }

        auto name = it->second->GetName();
        id_collections_.erase(it);
        name_collections_.erase(name);
        std::cout << ">>> [Remove] Collection " << id << std::endl;
        return true;
    }

    CollectionCommitPtr GetCollectionCommit(ID_TYPE id) {
        auto it = collection_commit_.find(id);
        if (it == collection_commit_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] CollectionCommit " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<CollectionCommit>(c->GetID(), c->GetCollectionId(),
                c->GetMappings(), c->GetStatus(), c->GetCreatedTime());
        return ret;
    }

    bool RemoveCollectionCommit(ID_TYPE id) {
        auto it = collection_commit_.find(id);
        if (it == collection_commit_.end()) {
            return false;
        }

        collection_commit_.erase(it);
        std::cout << ">>> [Remove] CollectionCommit " << id << std::endl;
        return true;
    }

    PartitionPtr GetPartition(ID_TYPE id) {
        auto it = partitions_.find(id);
        if (it == partitions_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] Partition " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<Partition>(c->GetID(), c->GetName(), c->GetCollectionId(),
                c->GetStatus(), c->GetCreatedTime());
        return ret;
    }

    bool RemovePartition(ID_TYPE id) {
        auto it = partitions_.find(id);
        if (it == partitions_.end()) {
            return false;
        }

        partitions_.erase(id);
        std::cout << ">>> [Remove] Partition " << id << std::endl;
        return true;
    }

    PartitionCommitPtr GetPartitionCommit(ID_TYPE id) {
        auto it = partition_commits_.find(id);
        if (it == partition_commits_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] PartitionCommit " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<PartitionCommit>(c->GetID(), c->GetCollectionId(), c->GetPartitionId(),
                c->GetMappings(), c->GetStatus(), c->GetCreatedTime());
        return ret;
    }

    bool RemovePartitionCommit(ID_TYPE id) {
        auto it = partition_commits_.find(id);
        if (it == partition_commits_.end()) {
            return false;
        }

        partition_commits_.erase(id);
        std::cout << ">>> [Remove] PartitionCommit " << id << std::endl;
        return true;
    }

    SegmentPtr GetSegment(ID_TYPE id) {
        auto it = segments_.find(id);
        if (it == segments_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] Segment " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<Segment>(c->GetID(), c->GetPartitionId(),
                c->GetStatus(), c->GetCreatedTime());
        return ret;
    }

    bool RemoveSegment(ID_TYPE id) {
        auto it = segments_.find(id);
        if (it == segments_.end()) {
            return false;
        }

        segments_.erase(id);
        std::cout << ">>> [Remove] Segment " << id << std::endl;
        return true;
    }

    SegmentCommitPtr GetSegmentCommit(ID_TYPE id) {
        auto it = segment_commits_.find(id);
        if (it == segment_commits_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] SegmentCommit " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<SegmentCommit>(c->GetID(), c->GetSchemaId(), c->GetPartitionId(),
                c->GetSegmentId(), c->GetMappings(), c->GetStatus(), c->GetCreatedTime());
        return ret;
    }

    bool RemoveSegmentCommit(ID_TYPE id) {
        auto it = segment_commits_.find(id);
        if (it == segment_commits_.end()) {
            return false;
        }

        segment_commits_.erase(id);
        std::cout << ">>> [Remove] SegmentCommit " << id << std::endl;
        return true;
    }

private:
    Store() {
        srand(time(0));
        int random;
        random = rand() % 2 + 4;
        int p_i = 0;
        int s_i = 0;
        int schema_id = 1;
        for (auto i=1; i<=random; i++) {
            std::stringstream name;
            name << "c_" << i;
            auto c = std::make_shared<Collection>(i, name.str());
            id_collections_[i] = c;
            name_collections_[name.str()] = c;

            auto c_c = std::make_shared<CollectionCommit>(i, i);
            collection_commit_[i] = c_c;

            int random_partitions = rand() % 2 + 1;
            for (auto pi=1; pi<=random_partitions; ++pi) {
                p_i++;
                std::stringstream pname;
                pname << "p_" << i << "_" << pi;
                auto p = std::make_shared<Partition>(p_i, pname.str(), c->GetID());

                partitions_[p_i] = p;
                auto p_c = std::make_shared<PartitionCommit>(p_i, c->GetID(), p_i);
                partition_commits_[p_i] = p_c;
                auto& c_c_m = c_c->GetMappings();
                c_c_m.push_back(p_i);

                int random_segments = rand() % 2 + 1;
                for (auto si=1; si<=random_segments; ++si) {
                    s_i++;
                    auto s = std::make_shared<Segment>(s_i, p->GetID());
                    segments_[s_i] = s;
                    auto s_c = std::make_shared<SegmentCommit>(s_i, schema_id, p->GetID(), s->GetID());
                    segment_commits_[s_c->GetID()] = s_c;
                    auto& p_c_m = p_c->GetMappings();
                    p_c_m.push_back(s_c->GetID());
                }
            }
        }
    }

    std::map<ID_TYPE, CollectionPtr> id_collections_;
    std::map<std::string, CollectionPtr> name_collections_;

    std::map<ID_TYPE, CollectionCommitPtr> collection_commit_;
    std::map<ID_TYPE, PartitionPtr> partitions_;
    std::map<ID_TYPE, PartitionCommitPtr> partition_commits_;

    std::map<ID_TYPE, SegmentPtr> segments_;
    std::map<ID_TYPE, SegmentCommitPtr> segment_commits_;
};
