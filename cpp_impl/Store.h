#pragma once
#include "Resources.h"
#include "Schema.h"
#include "schema.pb.h"

#include <stdlib.h>
#include <time.h>
#include <sstream>
#include <any>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <functional>
#include <iomanip>

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
        auto t = std::make_tuple(std::forward<ResourceT>(resources)...);
        auto& t_size = std::tuple_size<decltype(t)>::value;
        if (t_size == 0) return false;
        StartTransanction();
        std::apply([this](auto&&... resource) {((std::cout << CommitResource(resource) << "\n"), ...);}, t);
        FinishTransaction();
        return true;
    }

    template <typename OpT>
    void DoCommitOperation(OpT& op) {
        for(auto& step_v : op.GetSteps()) {
            auto id = ProcessOperationStep(step_v);
            op.SetStepResult(id);
        }
    }

    void StartTransanction() {}
    void FinishTransaction() {}

    template<typename ResourceT>
    bool CommitResource(ResourceT&& resource) {
        std::cout << "Commit " << resource.Name << " " << resource.GetID() << std::endl;
        auto res = CreateResource<typename std::remove_reference<ResourceT>::type>(std::move(resource));
        if (!res) return false;
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
        auto& id = std::get<Index<Collection::MapT, MockResourcesT>::value>(ids_);
        c->SetID(++id);
        resources[c->GetID()] = c;
        name_collections_[c->GetName()] = c;
        return GetResource<Collection>(c->GetID());
    }

    template <typename ResourceT>
    typename ResourceT::Ptr
    UpdateResource(ResourceT&& resource) {
        auto& resources = std::get<typename ResourceT::MapT>(resources_);
        auto res = std::make_shared<ResourceT>(resource);
        auto& id = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(ids_);
        resources[res->GetID()] = res;
        return GetResource<ResourceT>(res->GetID());
    }


    template <typename ResourceT>
    typename ResourceT::Ptr
    CreateResource(ResourceT&& resource) {
        if (resource.HasAssigned()) {
            return UpdateResource<ResourceT>(std::move(resource));
        }
        auto& resources = std::get<typename ResourceT::MapT>(resources_);
        auto res = std::make_shared<ResourceT>(resource);
        auto& id = std::get<Index<typename ResourceT::MapT, MockResourcesT>::value>(ids_);
        res->SetID(++id);
        resources[res->GetID()] = res;
        return GetResource<ResourceT>(res->GetID());
    }

    CollectionPtr CreateCollection(const schema::CollectionSchemaPB& collection_schema) {
        auto collection = CreateCollection(Collection(collection_schema.name()));
        IDS_TYPE field_commit_ids = {};
        for (auto i=0; i<collection_schema.fields_size(); ++i) {
            auto field_schema = collection_schema.fields(i);
            auto& field_name = field_schema.name();
            auto& field_info = field_schema.info();
            auto field_type = field_info.type();
            auto field = CreateResource<Field>(Field(field_name, i));
            IDS_TYPE element_ids = {};
            auto raw_element = CreateResource<FieldElement>(FieldElement(collection->GetID(),
                        field->GetID(), "RAW", 1));
            element_ids.push_back(raw_element->GetID());
            for(auto j=0; j<field_schema.elements_size(); ++j) {
                auto element_schema = field_schema.elements(j);
                auto& element_name = element_schema.name();
                auto& element_info = element_schema.info();
                auto element_type = element_info.type();
                auto element = CreateResource<FieldElement>(FieldElement(collection->GetID(), field->GetID(),
                            element_name, element_type));
                element_ids.push_back(element->GetID());
            }
            auto field_commit = CreateResource<FieldCommit>(FieldCommit(collection->GetID(), field->GetID(), element_ids));
            field_commit_ids.push_back(field_commit->GetID());
        }
        auto schema_commit = CreateResource<SchemaCommit>(SchemaCommit(collection->GetID(), field_commit_ids));

        IDS_TYPE empty_mappings = {};
        auto partition = CreateResource<Partition>(Partition("_default", collection->GetID()));
        auto partition_commit = CreateResource<PartitionCommit>(PartitionCommit(collection->GetID(), partition->GetID(),
                    empty_mappings));
        auto collection_commit = CreateResource<CollectionCommit>(CollectionCommit(collection->GetID(),
                    schema_commit->GetID(), {partition_commit->GetID()}));
        return collection;
    }

    void Mock() { DoMock(); }

private:

    ID_TYPE ProcessOperationStep(const std::any& step_v) {
        if (const auto it = any_vistors_.find(std::type_index(step_v.type()));
                it != any_vistors_.cend()) {
            return it->second(step_v);
        } else {
            std::cerr << "Unregisted step type " << std::quoted(step_v.type().name());
            return 0;
        }
    }

    template<class T, class F>
    inline std::pair<const std::type_index, std::function<ID_TYPE(std::any const&)>>
        to_any_visitor(F const &f)
    {
        return {
            std::type_index(typeid(T)),
            [g = f](std::any const &a) -> ID_TYPE
            {
                if constexpr (std::is_void_v<T>)
                    return g();
                else
                    return g(std::any_cast<T const&>(a));
            }
        };
    }

    template<class T, class F>
    inline void register_any_visitor(F const& f)
    {
        std::cout << "Register visitor for type "
                  << std::quoted(typeid(T).name()) << '\n';
        any_vistors_.insert(to_any_visitor<T>(f));
    }

    Store() {
        register_any_visitor<Collection::Ptr>([this](auto c) {
            auto n = CreateResource<Collection>(Collection(*c));
            return n->GetID();
        });
        /* register_any_visitor<CollectionCommit::Ptr>([this](auto c) { */
        /*     CreateResource<CollectionCommit>(CollectionCommit(*c)); */
        /* }); */
        /* register_any_visitor<SchemaCommit::Ptr>([this](auto c) { */
        /*     CreateResource<SchemaCommit>(SchemaCommit(*c)); */
        /* }); */
        /* register_any_visitor<FieldCommit::Ptr>([this](auto c) { */
        /*     CreateResource<FieldCommit>(FieldCommit(*c)); */
        /* }); */
        /* register_any_visitor<Field::Ptr>([this](auto c) { */
        /*     CreateResource<Field>(Field(*c)); */
        /* }); */
        /* register_any_visitor<FieldElement::Ptr>([this](auto c) { */
        /*     CreateResource<FieldElement>(FieldElement(*c)); */
        /* }); */
        /* register_any_visitor<PartitionCommit::Ptr>([this](auto c) { */
        /*     CreateResource<PartitionCommit>(PartitionCommit(*c)); */
        /* }); */
        /* register_any_visitor<Partition::Ptr>([this](auto c) { */
        /*     CreateResource<Partition>(Partition(*c)); */
        /* }); */
        /* register_any_visitor<Segment::Ptr>([this](auto c) { */
        /*     CreateResource<Segment>(Segment(*c)); */
        /* }); */
        /* register_any_visitor<SegmentCommit::Ptr>([this](auto c) { */
        /*     CreateResource<SegmentCommit>(SegmentCommit(*c)); */
        /* }); */
        /* register_any_visitor<SegmentFile::Ptr>([this](auto c) { */
        /*     CreateResource<SegmentFile>(SegmentFile(*c)); */
        /* }); */
    }

    void DoMock() {
        srand(time(0));
        int random;
        random = rand() % 2 + 4;
        IDS_TYPE empty_mappings = {};
        std::vector<std::any> all_records;
        for (auto i=1; i<=random; i++) {
            std::stringstream name;
            name << "c_" << std::get<Index<Collection::MapT, MockResourcesT>::value>(ids_) + 1;

            auto c = CreateCollection(Collection(name.str()));
            all_records.push_back(c);

            IDS_TYPE schema_c_m;
            int random_fields = rand() % 2 + 1;
            for (auto fi=1; fi<=random_fields; ++fi) {
                std::stringstream fname;
                fname << "f_" << fi << "_" << std::get<Index<Field::MapT, MockResourcesT>::value>(ids_) + 1;
                auto field = CreateResource<Field>(Field(fname.str(), fi));
                all_records.push_back(field);
                IDS_TYPE f_c_m = {};

                int random_elements = rand() % 2 + 2;
                for (auto fei=1; fei<=random_elements; ++fei) {
                    std::stringstream fename;
                    fename << "fe_" << fei << "_" << std::get<Index<FieldElement::MapT, MockResourcesT>::value>(ids_) + 1;

                    auto element = CreateResource<FieldElement>(FieldElement(c->GetID(), field->GetID(), fename.str(), fei));
                    all_records.push_back(element);
                    f_c_m.push_back(element->GetID());
                }
                auto f_c = CreateResource<FieldCommit>(FieldCommit(c->GetID(), field->GetID(), f_c_m));
                all_records.push_back(f_c);
                schema_c_m.push_back(f_c->GetID());
            }

            auto schema = CreateResource<SchemaCommit>(SchemaCommit(c->GetID(), schema_c_m));
            all_records.push_back(schema);


            int random_partitions = rand() % 2 + 1;
            IDS_TYPE c_c_m;
            for (auto pi=1; pi<=random_partitions; ++pi) {
                std::stringstream pname;
                pname << "p_" << i << "_" << std::get<Index<Partition::MapT, MockResourcesT>::value>(ids_) + 1;
                auto p = CreateResource<Partition>(Partition(pname.str(), c->GetID()));
                all_records.push_back(p);


                int random_segments = rand() % 2 + 1;
                IDS_TYPE p_c_m;
                for (auto si=1; si<=random_segments; ++si) {
                    auto s = CreateResource<Segment>(Segment(p->GetID()));
                    all_records.push_back(s);
                    auto& schema_m = schema->GetMappings();
                    IDS_TYPE s_c_m;
                    for (auto field_commit_id : schema_m) {
                        auto& field_commit = std::get<FieldCommit::MapT>(resources_)[field_commit_id];
                        auto& f_c_m = field_commit->GetMappings();
                        for (auto field_element_id : f_c_m) {
                            auto sf = CreateResource<SegmentFile>(SegmentFile(p->GetID(), s->GetID(), field_commit_id));
                            all_records.push_back(sf);

                            s_c_m.push_back(sf->GetID());
                        }
                    }
                    auto s_c = CreateResource<SegmentCommit>(SegmentCommit(schema->GetID(), p->GetID(), s->GetID(), s_c_m));
                    all_records.push_back(s_c);
                    p_c_m.push_back(s_c->GetID());
                }
                auto p_c = CreateResource<PartitionCommit>(PartitionCommit(c->GetID(), p->GetID(), p_c_m));
                all_records.push_back(p_c);
                c_c_m.push_back(p_c->GetID());
            }
            auto c_c = CreateResource<CollectionCommit>(CollectionCommit(c->GetID(), schema->GetID(), c_c_m));
            all_records.push_back(c_c);
        }
        for (auto& record : all_records) {
            if (record.type() == typeid(std::shared_ptr<Collection>)) {
                const auto& r = std::any_cast<std::shared_ptr<Collection>>(record);
                r->Activate();
            } else if (record.type() == typeid(std::shared_ptr<CollectionCommit>)) {
                const auto& r = std::any_cast<std::shared_ptr<CollectionCommit>>(record);
                r->Activate();
            }
        }
    }

    MockResourcesT resources_;
    MockIDST ids_;
    std::map<std::string, CollectionPtr> name_collections_;
    std::unordered_map<std::type_index, std::function<ID_TYPE(std::any const&)>> any_vistors_;
};
