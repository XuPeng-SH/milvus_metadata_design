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

    SchemaCommitPtr GetSchemaCommit(ID_TYPE id) {
        auto it = schema_commits_.find(id);
        if (it == schema_commits_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] SchemaCommit " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<SchemaCommit>(*c);
        return ret;
    }

    bool RemoveSchemaCommit(ID_TYPE id) {
        auto it = schema_commits_.find(id);
        if (it == schema_commits_.end()) {
            return false;
        }

        schema_commits_.erase(it);
        std::cout << ">>> [Remove] SchemaCommit " << id << std::endl;
        return true;
    }

    FieldCommitPtr GetFieldCommit(ID_TYPE id) {
        auto it = field_commits_.find(id);
        if (it == field_commits_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] FieldCommit " << id << std::endl;
        auto& c = it->second;
        return std::make_shared<FieldCommit>(*c);
    }

    bool RemoveFieldCommit(ID_TYPE id) {
        auto it = field_commits_.find(id);
        if (it == field_commits_.end()) {
            return false;
        }

        field_commits_.erase(it);
        std::cout << ">>> [Remove] FieldCommit " << id << std::endl;
        return true;
    }

    FieldPtr GetField(ID_TYPE id) {
        auto it = fields_.find(id);
        if (it == fields_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] Field " << id << std::endl;
        auto& c = it->second;
        return std::make_shared<Field>(*c);
    }

    bool RemoveField(ID_TYPE id) {
        auto it = fields_.find(id);
        if (it == fields_.end()) {
            return false;
        }

        fields_.erase(it);
        std::cout << ">>> [Remove] Field " << id << std::endl;
        return true;
    }

    FieldElementPtr GetFieldElement(ID_TYPE id) {
        auto it = field_elements_.find(id);
        if (it == field_elements_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] FieldElement " << id << std::endl;
        auto& c = it->second;
        return std::make_shared<FieldElement>(*c);
    }

    bool RemoveFieldElement(ID_TYPE id) {
        auto it = field_elements_.find(id);
        if (it == field_elements_.end()) {
            return false;
        }

        field_elements_.erase(it);
        std::cout << ">>> [Remove] FieldElement " << id << std::endl;
        return true;
    }

    CollectionCommitPtr GetCollectionCommit(ID_TYPE id) {
        auto it = collection_commit_.find(id);
        if (it == collection_commit_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] CollectionCommit " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<CollectionCommit>(c->GetID(), c->GetCollectionId(), c->GetSchemaId(),
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

    SegmentFilePtr GetSegmentFile(ID_TYPE id) {
        auto it = segment_files_.find(id);
        if (it == segment_files_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] SegmentFile " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<SegmentFile>(c->GetID(), c->GetPartitionId(), c->GetSegmentId(),
                c->GetFieldElementId(), c->GetStatus(), c->GetCreatedTime());
        return ret;
    }

    bool RemoveSegmentFile(ID_TYPE id) {
        auto it = segment_files_.find(id);
        if (it == segment_files_.end()) {
            return false;
        }

        segment_files_.erase(id);
        std::cout << ">>> [Remove] SegmentFile " << id << std::endl;
        return true;
    }

    IDS_TYPE AllActiveCollectionIds(bool reversed = true) const {
        IDS_TYPE ids;
        if (!reversed) {
            for (auto& kv : id_collections_) {
                ids.push_back(kv.first);
            }
        } else {
            for (auto kv = id_collections_.rbegin(); kv != id_collections_.rend(); ++kv) {
                ids.push_back(kv->first);
            }
        }
        return ids;
    }

    IDS_TYPE AllActiveCollectionCommitIds(bool reversed = true) const {
        IDS_TYPE ids;
        if (!reversed) {
            for (auto& kv : collection_commit_) {
                ids.push_back(kv.first);
            }
        } else {
            for (auto kv = collection_commit_.rbegin(); kv != collection_commit_.rend(); ++kv) {
                ids.push_back(kv->first);
            }
        }
        return ids;
    }

private:
    Store() {
        srand(time(0));
        int random;
        random = rand() % 2 + 4;
        int p_i = 0;
        int s_i = 0;
        int s_f_i = 0;
        int f_i = 0;
        int f_e_i = 0;
        /* int field_element_id = 1; */
        for (auto i=1; i<=random; i++) {
            std::stringstream name;
            name << "c_" << i;
            auto c = std::make_shared<Collection>(i, name.str());
            id_collections_[i] = c;
            name_collections_[name.str()] = c;

            auto schema = std::make_shared<SchemaCommit>(i, i);
            auto& schema_c_m = schema->GetMappings();
            int random_fields = rand() % 2 + 1;
            for (auto fi=1; fi<=random_fields; ++fi) {
                f_i++;
                std::stringstream fname;
                fname << "f_" << fi << "_" << f_i;
                auto field = std::make_shared<Field>(f_i, fname.str(), (NUM_TYPE)fi);
                fields_[field->GetID()] = field;

                auto f_c = std::make_shared<FieldCommit>(f_i, c->GetID(), field->GetID());
                field_commits_[f_c->GetID()] = f_c;
                schema_c_m.push_back(f_c->GetID());

                auto& f_c_m = f_c->GetMappings();
                int random_elements = rand() % 2 + 2;
                for (auto fei=1; fei<=random_elements; ++fei) {
                    f_e_i++;
                    std::stringstream fename;
                    fename << "fe_" << fei << "_" << f_e_i;

                    auto element = std::make_shared<FieldElement>(f_e_i, c->GetID(), field->GetID(), fename.str(), fei);
                    field_elements_[element->GetID()] = element;
                    f_c_m.push_back(element->GetID());
                }
            }

            schema_commits_[i] = schema;

            auto c_c = std::make_shared<CollectionCommit>(i, i, schema->GetID());
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
                    auto s_c = std::make_shared<SegmentCommit>(s_i, schema->GetID(), p->GetID(), s->GetID());
                    segment_commits_[s_c->GetID()] = s_c;
                    auto& p_c_m = p_c->GetMappings();
                    p_c_m.push_back(s_c->GetID());
                    int random_seg_files = rand() % 2 + 1;
                    auto& schema_m = schema->GetMappings();
                    for (auto field_commit_id : schema_m) {
                        auto& field_commit = field_commits_[field_commit_id];
                        auto& f_c_m = field_commit->GetMappings();
                        for (auto field_element_id : f_c_m) {
                            s_f_i++;
                            auto sf = std::make_shared<SegmentFile>(s_f_i, p->GetID(), s->GetID(), field_element_id);
                            segment_files_[sf->GetID()] = sf;

                            auto& s_c_m = s_c->GetMappings();
                            s_c_m.push_back(sf->GetID());
                        }
                    }
                }
            }
        }
    }

    std::map<ID_TYPE, CollectionPtr> id_collections_;
    std::map<std::string, CollectionPtr> name_collections_;

    std::map<ID_TYPE, SchemaCommitPtr> schema_commits_;
    std::map<ID_TYPE, FieldCommitPtr> field_commits_;
    std::map<ID_TYPE, FieldPtr> fields_;
    std::map<ID_TYPE, FieldElementPtr> field_elements_;

    std::map<ID_TYPE, CollectionCommitPtr> collection_commit_;
    std::map<ID_TYPE, PartitionPtr> partitions_;
    std::map<ID_TYPE, PartitionCommitPtr> partition_commits_;

    std::map<ID_TYPE, SegmentPtr> segments_;
    std::map<ID_TYPE, SegmentCommitPtr> segment_commits_;

    std::map<ID_TYPE, SegmentFilePtr> segment_files_;
};
