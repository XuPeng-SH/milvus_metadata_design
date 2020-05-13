#pragma once
#include "Resources.h"
#include "Schema.h"
#include "schema.pb.h"

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
        auto ret = std::make_shared<Collection>(*c);
        return ret;
    }

    CollectionPtr GetCollection(const std::string& name) {
        auto it = name_collections_.find(name);
        if (it == name_collections_.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] Collection " << name << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<Collection>(*c);
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
        auto ret = std::make_shared<CollectionCommit>(*c);
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
        auto ret = std::make_shared<Partition>(*c);
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
        auto ret = std::make_shared<PartitionCommit>(*c);
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
        auto ret = std::make_shared<Segment>(*c);
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
        auto ret = std::make_shared<SegmentCommit>(*c);
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
        auto ret = std::make_shared<SegmentFile>(*c);
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

    IDS_TYPE AllActiveCollectionCommitIds(ID_TYPE collection_id, bool reversed = true) const {
        IDS_TYPE ids;
        if (!reversed) {
            for (auto& kv : collection_commit_) {
                if (kv.second->GetCollectionId() == collection_id) {
                    ids.push_back(kv.first);
                }
            }
        } else {
            for (auto kv = collection_commit_.rbegin(); kv != collection_commit_.rend(); ++kv) {
                if (kv->second->GetCollectionId() == collection_id) {
                    ids.push_back(kv->first);
                }
            }
        }
        return ids;
    }

    CollectionPtr CreateCollection(Collection&& collection) {
        auto c = std::make_shared<Collection>(collection);
        c->SetID(++c_id_);
        id_collections_[c->GetID()] = c;
        name_collections_[c->GetName()] = c;
        return GetCollection(c->GetID());
    }

    CollectionPtr CreateCollection(const Collection& collection) {
        auto ret = std::make_shared<Collection>(collection);
        ret->SetID(++c_id_);
        id_collections_[ret->GetID()] = ret;
        name_collections_[ret->GetName()] = ret;
        return GetCollection(ret->GetID());
    }

    FieldPtr CreateField(Field&& field) {
        auto f = std::make_shared<Field>(field);
        f->SetID(++f_id_);
        fields_[f->GetID()] = f;
        return GetField(f->GetID());
    }

    CollectionPtr CreateCollection(const schema::CollectionSchemaPB& collection_schema) {
        auto collection = CreateCollection(Collection(collection_schema.name()));
        for (auto i=0; i<collection_schema.fields_size(); ++i) {
            auto field_schema = collection_schema.fields(i);
            auto& field_name = field_schema.name();
            auto& field_info = field_schema.info();
            auto field_type = field_info.type();
            auto field = CreateField(Field(field_name, i));
        }
    }

private:
    Store() {
        srand(time(0));
        int random;
        random = rand() % 2 + 4;
        IDS_TYPE empty_mappings = {};
        /* int field_element_id = 1; */
        for (auto i=1; i<=random; i++) {
            c_c_id_++;
            s_c_id_++;
            std::stringstream name;
            name << "c_" << (c_id_ + 1);

            auto c = CreateCollection(Collection(name.str()));

            auto schema = std::make_shared<SchemaCommit>(c->GetID(), empty_mappings, s_c_id_);
            auto& schema_c_m = schema->GetMappings();
            int random_fields = rand() % 2 + 1;
            for (auto fi=1; fi<=random_fields; ++fi) {
                f_c_id_++;
                std::stringstream fname;
                fname << "f_" << fi << "_" << f_id_ + 1;
                auto field = CreateField(Field(fname.str(), fi));

                auto f_c = std::make_shared<FieldCommit>(c->GetID(), field->GetID(), empty_mappings, f_c_id_);
                field_commits_[f_c->GetID()] = f_c;
                schema_c_m.push_back(f_c->GetID());

                auto& f_c_m = f_c->GetMappings();
                int random_elements = rand() % 2 + 2;
                for (auto fei=1; fei<=random_elements; ++fei) {
                    f_e_id_++;
                    std::stringstream fename;
                    fename << "fe_" << fei << "_" << f_e_id_;

                    auto element = std::make_shared<FieldElement>(c->GetID(), field->GetID(), fename.str(), fei, f_e_id_);
                    field_elements_[element->GetID()] = element;
                    f_c_m.push_back(element->GetID());
                }
            }

            schema_commits_[schema->GetID()] = schema;

            auto c_c = std::make_shared<CollectionCommit>(c->GetID(), schema->GetID(), empty_mappings, c_c_id_);
            collection_commit_[c_c->GetID()] = c_c;

            int random_partitions = rand() % 2 + 1;
            for (auto pi=1; pi<=random_partitions; ++pi) {
                p_id_++;
                p_c_id_++;
                std::stringstream pname;
                pname << "p_" << i << "_" << pi;
                auto p = std::make_shared<Partition>(pname.str(), c->GetID(), p_id_);

                partitions_[p_id_] = p;
                auto p_c = std::make_shared<PartitionCommit>(c->GetID(), p->GetID(), empty_mappings, p_c_id_);
                partition_commits_[p_c->GetID()] = p_c;
                auto& c_c_m = c_c->GetMappings();
                c_c_m.push_back(p_c->GetID());

                int random_segments = rand() % 2 + 1;
                for (auto si=1; si<=random_segments; ++si) {
                    seg_id_++;
                    seg_c_id_++;
                    auto s = std::make_shared<Segment>(p->GetID(), seg_id_);
                    segments_[seg_id_] = s;
                    auto s_c = std::make_shared<SegmentCommit>(schema->GetID(), p->GetID(), s->GetID(), empty_mappings, seg_c_id_);
                    segment_commits_[s_c->GetID()] = s_c;
                    auto& p_c_m = p_c->GetMappings();
                    p_c_m.push_back(s_c->GetID());
                    int random_seg_files = rand() % 2 + 1;
                    auto& schema_m = schema->GetMappings();
                    for (auto field_commit_id : schema_m) {
                        auto& field_commit = field_commits_[field_commit_id];
                        auto& f_c_m = field_commit->GetMappings();
                        for (auto field_element_id : f_c_m) {
                            seg_f_id_++;
                            auto sf = std::make_shared<SegmentFile>(p->GetID(), s->GetID(), field_element_id, seg_f_id_);
                            segment_files_[sf->GetID()] = sf;

                            auto& s_c_m = s_c->GetMappings();
                            s_c_m.push_back(sf->GetID());
                        }
                    }
                }
            }
        }
    }

    ID_TYPE c_id_ = 0;
    ID_TYPE s_c_id_ = 0;
    ID_TYPE f_id_ = 0;
    ID_TYPE f_c_id_ = 0;
    ID_TYPE f_e_id_ = 0;
    ID_TYPE c_c_id_ = 0;
    ID_TYPE p_id_ = 0;
    ID_TYPE p_c_id_ = 0;
    ID_TYPE seg_id_ = 0;
    ID_TYPE seg_c_id_ = 0;
    ID_TYPE seg_f_id_ = 0;
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
