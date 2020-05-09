#pragma once
#include <functional>
#include <vector>
#include <memory>

using OnNoRefCBF = std::function<void(void)>;

class ReferenceProxy {
public:
    void RegisterOnNoRefCB(OnNoRefCBF cb);

    virtual void Ref();
    virtual void UnRef();

    int RefCnt() const { return refcnt_; }

    virtual ~ReferenceProxy();

protected:

    int refcnt_ = 0;
    std::vector<OnNoRefCBF> on_no_ref_cbs_;
};

using ReferenceResourcePtr = std::shared_ptr<ReferenceProxy>;
