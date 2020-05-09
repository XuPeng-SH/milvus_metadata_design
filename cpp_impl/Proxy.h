#pragma once
#include <memory>

template <typename ResourceT>
class ScopedResource {
public:
    using ThisT = ScopedResource<ResourceT>;
    using Ptr = std::shared_ptr<ThisT>;
    using ResourcePtr = typename ResourceT::Ptr;
    ScopedResource();
    ScopedResource(ResourcePtr res, bool scoped = true);

    ResourcePtr Get() { return res_; }

    ResourceT operator*() const { return *res_; }
    ResourcePtr operator->() const { return res_; }

    operator bool () const {
        if (res_) return true;
        else return false;
    }

    ~ScopedResource();

protected:
    ResourcePtr res_;
    bool scoped_;
};

template <typename ResourceT>
ScopedResource<ResourceT>::ScopedResource() : res_(nullptr), scoped_(false) {
}

template <typename ResourceT>
ScopedResource<ResourceT>::ScopedResource(ScopedResource<ResourceT>::ResourcePtr res,
        bool scoped) : res_(res), scoped_(scoped) {
    if (scoped) {
        res_->Ref();
    }
}

template <typename ResourceT>
ScopedResource<ResourceT>::~ScopedResource() {
    if (scoped_) {
        res_->UnRef();
    }
}
