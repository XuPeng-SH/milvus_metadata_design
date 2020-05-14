#pragma once
#include "Resources.h"
#include "Schema.h"
#include "schema.pb.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>

template <class T, class Tuple>
struct Index;

template <class T, class... Types>
struct Index<T, std::tuple<T, Types...>> {
    static const std::size_t value = 0;
};

template <class T, class U, class... Types>
struct Index<T, std::tuple<U, Types...>> {
    static const std::size_t value = 1 + Index<T, std::tuple<Types...>>::value;
};
    /* std::map<std::string, CollectionPtr> name_collections_; */

using CollectionMap = std::map<ID_TYPE, CollectionPtr>;
using SchemaCommitMap = std::map<ID_TYPE, SchemaCommitPtr>;
using FieldCommitMap = std::map<ID_TYPE, FieldCommitPtr>;
using FieldMap = std::map<ID_TYPE, FieldPtr>;
using FieldElementMap = std::map<ID_TYPE, FieldElementPtr>;

using CollectionCommitMap = std::map<ID_TYPE, CollectionCommitPtr>;
using PartitionMap = std::map<ID_TYPE, PartitionPtr>;
using PartitionCommitMap = std::map<ID_TYPE, PartitionCommitPtr>;

using SegmentMap = std::map<ID_TYPE, SegmentPtr>;
using SegmentCommitMap = std::map<ID_TYPE, SegmentCommitPtr>;

using SegmentFileMap = std::map<ID_TYPE, SegmentFilePtr>;


class Store {
public:
    using ResourcesT = std::tuple<CollectionCommit::MapT, SchemaCommit::MapT>;

    static Store& GetInstance() {
        static Store store;
        return store;
    }

    template<typename ...ResourceT>
    bool DoCommit(ResourceT&&... resources) {
        auto t = std::make_tuple(resources...);
        std::apply([this](auto&&... resource) {((std::cout << CommitResource(resource) << "\n"), ...);}, t);
        return true;
    }

    template<typename ResourceT>
    bool CommitResource(ResourceT&& resource) {
        std::cout << "Commit " << resource.Name << " " << resource.GetID() << std::endl;
        return true;
    }

    template<typename ResourceT>
    std::shared_ptr<ResourceT>
    GetResource(ID_TYPE id) {
        auto& resources = std::get<Index<typename ResourceT::MapT, ResourcesT>::value>(resources_);
        auto it = resources.find(id);
        if (it== resources.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] " << ResourceT::Name << " " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<ResourceT>(*c);
        return ret;
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

    template<typename ResourceT>
    bool RemoveResource(ID_TYPE id) {
        auto& resources = std::get<Index<typename ResourceT::MapT, ResourcesT>::value>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            return false;
        }

        resources.erase(it);
        std::cout << ">>> [Remove] " << ResourceT::Name << " " << id << std::endl;
        return true;
    }


    bool RemoveCollectionCommit(ID_TYPE id) {
        auto& resources = std::get<0>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            return false;
        }

        resources.erase(it);
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
        auto& resources = std::get<0>(resources_);
        if (!reversed) {
            for (auto& kv : resources) {
                if (kv.second->GetCollectionId() == collection_id) {
                    ids.push_back(kv.first);
                }
            }
        } else {
            for (auto kv = resources.rbegin(); kv != resources.rend(); ++kv) {
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

    FieldElementPtr CreateFieldElement(FieldElement&& field_element) {
        auto fe = std::make_shared<FieldElement>(field_element);
        fe->SetID(++f_e_id_);
        field_elements_[fe->GetID()] = fe;
        return GetFieldElement(fe->GetID());
    }

    FieldCommitPtr CreateFieldCommit(FieldCommit&& field_commit) {
        auto fc = std::make_shared<FieldCommit>(field_commit);
        fc->SetID(++f_c_id_);
        field_commits_[fc->GetID()] = fc;
        return GetFieldCommit(fc->GetID());
    }

    SchemaCommitPtr CreateSchemaCommit(SchemaCommit&& schema_commit) {
        auto& resources = std::get<1>(resources_);
        auto sc = std::make_shared<SchemaCommit>(schema_commit);
        sc->SetID(++s_c_id_);
        resources[sc->GetID()] = sc;
        return GetResource<SchemaCommit>(sc->GetID());
    }

    PartitionPtr CreatePartition(Partition&& partition) {
        auto p = std::make_shared<Partition>(partition);
        p->SetID(++p_id_);
        partitions_[p->GetID()] = p;
        return GetPartition(p->GetID());
    }

    PartitionCommitPtr CreatePartitionCommit(PartitionCommit&& partition_commit) {
        auto pc = std::make_shared<PartitionCommit>(partition_commit);
        pc->SetID(++p_c_id_);
        partition_commits_[pc->GetID()] = pc;
        return GetPartitionCommit(pc->GetID());
    }

    CollectionCommitPtr CreateCollectionCommit(CollectionCommit&& collection_commit) {
        auto& resources = std::get<0>(resources_);
        auto cc = std::make_shared<CollectionCommit>(collection_commit);
        cc->SetID(++c_c_id_);
        resources[cc->GetID()] = cc;
        return GetResource<CollectionCommit>(cc->GetID());
    }

    SegmentFilePtr CreateSegmentFile(SegmentFile&& segment_file) {
        auto sf = std::make_shared<SegmentFile>(segment_file);
        sf->SetID(++seg_f_id_);
        segment_files_[sf->GetID()] = sf;
        return GetSegmentFile(sf->GetID());
    }

    SegmentPtr CreateSegment(Segment&& segment) {
        auto s = std::make_shared<Segment>(segment);
        s->SetID(++seg_id_);
        segments_[s->GetID()] = s;
        return GetSegment(s->GetID());
    }

    SegmentCommitPtr CreateSegmentCommit(SegmentCommit&& segment_commit) {
        auto sc = std::make_shared<SegmentCommit>(segment_commit);
        sc->SetID(++seg_c_id_);
        segment_commits_[sc->GetID()] = sc;
        return GetSegmentCommit(sc->GetID());
    }

    CollectionPtr CreateCollection(const schema::CollectionSchemaPB& collection_schema) {
        auto collection = CreateCollection(Collection(collection_schema.name()));
        IDS_TYPE field_commit_ids = {};
        for (auto i=0; i<collection_schema.fields_size(); ++i) {
            auto field_schema = collection_schema.fields(i);
            auto& field_name = field_schema.name();
            auto& field_info = field_schema.info();
            auto field_type = field_info.type();
            auto field = CreateField(Field(field_name, i));
            IDS_TYPE element_ids = {};
            auto raw_element = CreateFieldElement(FieldElement(collection->GetID(),
                        field->GetID(), "RAW", 1));
            element_ids.push_back(raw_element->GetID());
            for(auto j=0; j<field_schema.elements_size(); ++j) {
                auto element_schema = field_schema.elements(j);
                auto& element_name = element_schema.name();
                auto& element_info = element_schema.info();
                auto element_type = element_info.type();
                auto element = CreateFieldElement(FieldElement(collection->GetID(), field->GetID(),
                            element_name, element_type));
                element_ids.push_back(element->GetID());
            }
            auto field_commit = CreateFieldCommit(FieldCommit(collection->GetID(), field->GetID(), element_ids));
            field_commit_ids.push_back(field_commit->GetID());
        }
        auto schema_commit = CreateSchemaCommit(SchemaCommit(collection->GetID(), field_commit_ids));

        IDS_TYPE empty_mappings = {};
        auto partition = CreatePartition(Partition("_default", collection->GetID()));
        auto partition_commit = CreatePartitionCommit(PartitionCommit(collection->GetID(), partition->GetID(),
                    empty_mappings));
        auto collection_commit = CreateCollectionCommit(CollectionCommit(collection->GetID(),
                    schema_commit->GetID(), {partition_commit->GetID()}));
        return collection;
    }

    void Mock() { DoMock(); }

private:
    Store() { }

    void DoMock() {
        srand(time(0));
        int random;
        random = rand() % 2 + 4;
        IDS_TYPE empty_mappings = {};
        /* int field_element_id = 1; */
        for (auto i=1; i<=random; i++) {
            std::stringstream name;
            name << "c_" << (c_id_ + 1);

            auto c = CreateCollection(Collection(name.str()));

            IDS_TYPE schema_c_m;
            int random_fields = rand() % 2 + 1;
            for (auto fi=1; fi<=random_fields; ++fi) {
                std::stringstream fname;
                fname << "f_" << fi << "_" << f_id_ + 1;
                auto field = CreateField(Field(fname.str(), fi));
                IDS_TYPE f_c_m = {};

                int random_elements = rand() % 2 + 2;
                for (auto fei=1; fei<=random_elements; ++fei) {
                    std::stringstream fename;
                    fename << "fe_" << fei << "_" << f_e_id_ + 1;

                    auto element = CreateFieldElement(FieldElement(c->GetID(), field->GetID(), fename.str(), fei));
                    f_c_m.push_back(element->GetID());
                }
                auto f_c = CreateFieldCommit(FieldCommit(c->GetID(), field->GetID(), f_c_m));
                schema_c_m.push_back(f_c->GetID());
            }

            auto schema = CreateSchemaCommit(SchemaCommit(c->GetID(), schema_c_m));
            /* for (auto ii : schema_c_m) { */
            /*     std::cout << "CID=" << c->GetID() << " SCID=" << schema->GetID() << " FCID=" << ii << std::endl; */
            /* } */

            auto c_c = CreateCollectionCommit(CollectionCommit(c->GetID(), schema->GetID(), empty_mappings));

            int random_partitions = rand() % 2 + 1;
            for (auto pi=1; pi<=random_partitions; ++pi) {
                std::stringstream pname;
                pname << "p_" << i << "_" << p_id_ + 1;
                auto p = CreatePartition(Partition(pname.str(), c->GetID()));

                auto p_c = CreatePartitionCommit(PartitionCommit(c->GetID(), p->GetID(), empty_mappings));
                auto& c_c_m = c_c->GetMappings();
                c_c_m.push_back(p_c->GetID());

                int random_segments = rand() % 2 + 1;
                for (auto si=1; si<=random_segments; ++si) {
                    auto s = CreateSegment(Segment(p->GetID()));
                    auto s_c = CreateSegmentCommit(SegmentCommit(schema->GetID(), p->GetID(), s->GetID(), empty_mappings));
                    auto& p_c_m = p_c->GetMappings();
                    p_c_m.push_back(s_c->GetID());
                    auto& schema_m = schema->GetMappings();
                    for (auto field_commit_id : schema_m) {
                        auto& field_commit = field_commits_[field_commit_id];
                        auto& f_c_m = field_commit->GetMappings();
                        for (auto field_element_id : f_c_m) {
                            auto sf = CreateSegmentFile(SegmentFile(p->GetID(), s->GetID(), field_commit_id));
                            /* std::cout << "\tP=" << p->GetID() << " FID=" << field_commit->GetFieldId() <<  " FEI=" << field_element_id << " SEG=" << s->GetID() << " SF=" << sf->GetID() << std::endl; */

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

    ResourcesT resources_;

    std::map<ID_TYPE, CollectionPtr> id_collections_;
    std::map<std::string, CollectionPtr> name_collections_;

    std::map<ID_TYPE, SchemaCommitPtr> schema_commits_;
    std::map<ID_TYPE, FieldCommitPtr> field_commits_;
    std::map<ID_TYPE, FieldPtr> fields_;
    std::map<ID_TYPE, FieldElementPtr> field_elements_;

    std::map<ID_TYPE, PartitionPtr> partitions_;
    std::map<ID_TYPE, PartitionCommitPtr> partition_commits_;

    std::map<ID_TYPE, SegmentPtr> segments_;
    std::map<ID_TYPE, SegmentCommitPtr> segment_commits_;

    std::map<ID_TYPE, SegmentFilePtr> segment_files_;
};
