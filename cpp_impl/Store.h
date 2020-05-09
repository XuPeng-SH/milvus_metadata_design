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

private:
    Store() {
        srand(time(0));
        int random;
        random = rand() % 5 + 5;
        for (auto i=1; i<=random; i++) {
            std::stringstream name;
            name << "c_" << i;
            auto c = std::make_shared<Collection>(i, name.str());
            id_collections_[i] = c;
            name_collections_[name.str()] = c;

            auto c_c = std::make_shared<CollectionCommit>(i, i);
            collection_commit_[i] = c_c;
        }
    }

    std::map<ID_TYPE, CollectionPtr> id_collections_;
    std::map<std::string, CollectionPtr> name_collections_;

    std::map<ID_TYPE, CollectionCommitPtr> collection_commit_;
};
