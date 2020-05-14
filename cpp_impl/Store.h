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
    using MockIDST = std::tuple<ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE,
                                ID_TYPE, ID_TYPE, ID_TYPE, ID_TYPE>;
    using MockResourcesT = std::tuple<CollectionCommit::MapT,
                                  Collection::MapT,
                                  SchemaCommit::MapT,
                                  FieldCommit::MapT,
                                  Field::MapT,
                                  FieldElement::MapT,
                                  PartitionCommit::MapT,
                                  Partition::MapT,
                                  SegmentCommit::MapT,
                                  Segment::MapT,
                                  SegmentFile::MapT>;

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
        auto& resources = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(resources_);
        auto it = resources.find(id);
        if (it== resources.end()) {
            return nullptr;
        }
        std::cout << "<<< [Load] " << ResourceT::Name << " " << id << std::endl;
        auto& c = it->second;
        auto ret = std::make_shared<ResourceT>(*c);
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
        auto& resources = std::get<Collection::MapT>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            return false;
        }

        auto name = it->second->GetName();
        resources.erase(it);
        name_collections_.erase(name);
        std::cout << ">>> [Remove] Collection " << id << std::endl;
        return true;
    }

    template<typename ResourceT>
    bool RemoveResource(ID_TYPE id) {
        auto& resources = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(resources_);
        auto it = resources.find(id);
        if (it == resources.end()) {
            return false;
        }

        resources.erase(it);
        std::cout << ">>> [Remove] " << ResourceT::Name << " " << id << std::endl;
        return true;
    }

    IDS_TYPE AllActiveCollectionIds(bool reversed = true) const {
        IDS_TYPE ids;
        auto& resources = std::get<Collection::MapT>(resources_);
        if (!reversed) {
            for (auto& kv : resources) {
                ids.push_back(kv.first);
            }
        } else {
            for (auto kv = resources.rbegin(); kv != resources.rend(); ++kv) {
                ids.push_back(kv->first);
            }
        }
        return ids;
    }

    IDS_TYPE AllActiveCollectionCommitIds(ID_TYPE collection_id, bool reversed = true) const {
        IDS_TYPE ids;
        auto& resources = std::get<CollectionCommit::MapT>(resources_);
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
        auto& resources = std::get<Collection::MapT>(resources_);
        auto c = std::make_shared<Collection>(collection);
        c->SetID(++c_id_);
        resources[c->GetID()] = c;
        name_collections_[c->GetName()] = c;
        return GetResource<Collection>(c->GetID());
    }

    FieldPtr CreateField(Field&& field) {
        auto& resources = std::get<Field::MapT>(resources_);
        auto f = std::make_shared<Field>(field);
        f->SetID(++f_id_);
        resources[f->GetID()] = f;
        return GetResource<Field>(f->GetID());
    }

    FieldElementPtr CreateFieldElement(FieldElement&& field_element) {
        auto& resources = std::get<FieldElement::MapT>(resources_);
        auto fe = std::make_shared<FieldElement>(field_element);
        fe->SetID(++f_e_id_);
        resources[fe->GetID()] = fe;
        return GetResource<FieldElement>(fe->GetID());
    }

    FieldCommitPtr CreateFieldCommit(FieldCommit&& field_commit) {
        auto& resources = std::get<FieldCommit::MapT>(resources_);
        auto fc = std::make_shared<FieldCommit>(field_commit);
        fc->SetID(++f_c_id_);
        resources[fc->GetID()] = fc;
        return GetResource<FieldCommit>(fc->GetID());
    }

    SchemaCommitPtr CreateSchemaCommit(SchemaCommit&& schema_commit) {
        auto& resources = std::get<SchemaCommit::MapT>(resources_);
        auto sc = std::make_shared<SchemaCommit>(schema_commit);
        sc->SetID(++s_c_id_);
        resources[sc->GetID()] = sc;
        return GetResource<SchemaCommit>(sc->GetID());
    }

    PartitionPtr CreatePartition(Partition&& partition) {
        auto& resources = std::get<Partition::MapT>(resources_);
        auto p = std::make_shared<Partition>(partition);
        p->SetID(++p_id_);
        resources[p->GetID()] = p;
        return GetResource<Partition>(p->GetID());
    }

    PartitionCommitPtr CreatePartitionCommit(PartitionCommit&& partition_commit) {
        auto& resources = std::get<PartitionCommit::MapT>(resources_);
        auto pc = std::make_shared<PartitionCommit>(partition_commit);
        pc->SetID(++p_c_id_);
        resources[pc->GetID()] = pc;
        return GetResource<PartitionCommit>(pc->GetID());
    }

    CollectionCommitPtr CreateCollectionCommit(CollectionCommit&& collection_commit) {
        auto& resources = std::get<CollectionCommit::MapT>(resources_);
        auto cc = std::make_shared<CollectionCommit>(collection_commit);
        cc->SetID(++c_c_id_);
        resources[cc->GetID()] = cc;
        return GetResource<CollectionCommit>(cc->GetID());
    }

    SegmentFilePtr CreateSegmentFile(SegmentFile&& segment_file) {
        auto& resources = std::get<SegmentFile::MapT>(resources_);
        auto sf = std::make_shared<SegmentFile>(segment_file);
        sf->SetID(++seg_f_id_);
        resources[sf->GetID()] = sf;
        return GetResource<SegmentFile>(sf->GetID());
    }

    SegmentPtr CreateSegment(Segment&& segment) {
        auto& resources = std::get<Segment::MapT>(resources_);
        auto s = std::make_shared<Segment>(segment);
        s->SetID(++seg_id_);
        resources[s->GetID()] = s;
        return GetResource<Segment>(s->GetID());
    }

    SegmentCommitPtr CreateSegmentCommit(SegmentCommit&& segment_commit) {
        auto& resources = std::get<SegmentCommit::MapT>(resources_);
        auto sc = std::make_shared<SegmentCommit>(segment_commit);
        sc->SetID(++seg_c_id_);
        resources[sc->GetID()] = sc;
        return GetResource<SegmentCommit>(sc->GetID());
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
                        auto& field_commit = std::get<FieldCommit::MapT>(resources_)[field_commit_id];
                        auto& f_c_m = field_commit->GetMappings();
                        for (auto field_element_id : f_c_m) {
                            auto sf = CreateSegmentFile(SegmentFile(p->GetID(), s->GetID(), field_commit_id));

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

    MockResourcesT resources_;
    MockIDST ids_;
    std::map<std::string, CollectionPtr> name_collections_;
};
