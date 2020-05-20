#pragma once

#include <string>
#include <vector>
#include <set>

using ID_TYPE = int64_t;
using NUM_TYPE = int64_t;
using FTYPE_TYPE = int64_t;
using TS_TYPE = int64_t;
using MappingT = std::set<ID_TYPE>;
/* using MappingT = std::vector<ID_TYPE>; */

using IDS_TYPE = std::vector<ID_TYPE>;

enum State {
    PENDING = 0,
    ACTIVE = 1,
    DEACTIVE = 2
};

/* struct CommonFields { */
/*     ID_TYPE id_; */
/*     State status_; */
/*     TS_TYPE created_on_; */
/* } */

/* struct Collection : public CommonFields { */
/*     std::string name_; */
/* }; */

/* struct CollectionCommit : public CommonFields { */
/*     MappingT mappings_; */
/*     ID_TYPE collection_id_; */
/* } */
