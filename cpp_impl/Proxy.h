#pragma once
#include <memory>

template <typename ResourceT>
class ScopedResource {
public:
    using ThisT = ScopedResource<ResourceT>;
    using Ptr = std::shared_ptr<ThisT>;
    using ResourcePtr = std::shared_ptr<ResourceT>;
    /* using ResourcePtr = typename ResourceT::Ptr; */
    ScopedResource();
    ScopedResource(ResourcePtr res, bool scoped = true);

    ScopedResource(const ScopedResource<ResourceT>& res);

    ScopedResource<ResourceT>& operator=(const ScopedResource<ResourceT>& res);

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
        /* std::cout << "Do Ref" << std::endl; */
        res_->Ref();
    }
}


template <typename ResourceT>
ScopedResource<ResourceT>&
ScopedResource<ResourceT>::operator=(const ScopedResource<ResourceT>& res) {
    if (this == &res) return *this;
    if (scoped_) {
        res_->UnRef();
    }
    res_ = res.res_;
    scoped_ = res.scoped_;
    if (scoped_) {
        res_->Ref();
    }
    return *this;
}

template <typename ResourceT>
ScopedResource<ResourceT>::ScopedResource(const ScopedResource<ResourceT>& res) {
    res_ = res.res_;
    if (res.scoped_) {
        res_->Ref();
    }
    scoped_ = res.scoped_;
}

template <typename ResourceT>
ScopedResource<ResourceT>::~ScopedResource() {
    if (scoped_) {
        /* std::cout << "Do UnRef" << std::endl; */
        res_->UnRef();
    }
}
