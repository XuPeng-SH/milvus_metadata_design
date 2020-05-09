#include "Resources.h"
#include "Store.h"
#include <sstream>
#include <iostream>

template <typename Derived>
DBBaseResource<Derived>::DBBaseResource(ID_TYPE id, State status, TS_TYPE created_on) :
    id_(id), status_(status), created_on_(created_on) {
}

template <typename Derived>
std::string DBBaseResource<Derived>::ToString() const {
    std::stringstream ss;
    ss << "ID=" << id_ << ", Status=" << status_ << ", TS=" << created_on_;
    return ss.str();
}

Collection::Collection(ID_TYPE id, const std::string& name, State status, TS_TYPE created_on) :
    BaseT(id, status, created_on),
    NameMixin(name) {
}

std::string Collection::ToString() const {
    std::stringstream ss;
    ss << "<" << BaseT::ToString() << ", Name=" << name_ << ">";
    return ss.str();
}

CollectionCommit::CollectionCommit(ID_TYPE id, ID_TYPE collection_id,
        const MappingT& mappings, State status, TS_TYPE created_on) :
    BaseT(id, status, created_on), MappingsMixin(mappings), CollectionIdMixin(collection_id) {
}

std::string CollectionCommit::ToString() const {
    std::stringstream ss;
    ss << "<" << BaseT::ToString() << ", Mappings=" << "[";
    bool first = true;
    std::string prefix;
    for (auto& id : mappings_) {
        if (!first) prefix = ", ";
        else first = false;
        ss << prefix << id;
    }
    ss << "]>";
    return ss.str();
}
