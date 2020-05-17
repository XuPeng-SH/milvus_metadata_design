#pragma once
#include "SnapshotHolder.h"
#include "schema.pb.h"
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <mutex>
#include <shared_mutex>
#include <thread>
#include <atomic>


class Snapshots {
public:
    static Snapshots& GetInstance() {
        static Snapshots sss;
        return sss;
    }
    bool Close(ID_TYPE collection_id);
    SnapshotHolderPtr GetHolder(ID_TYPE collection_id);
    SnapshotHolderPtr GetHolder(const std::string& name);

    ScopedSnapshotT GetSnapshot(ID_TYPE collection_id, ID_TYPE id = 0, bool scoped = true);
    ScopedSnapshotT GetSnapshot(const std::string& name, ID_TYPE id = 0, bool scoped = true);

    IDS_TYPE GetCollectionIds() const;

    bool DropCollection(const std::string& name);

    template<typename ...ResourceT>
    bool Flush(ResourceT&&... resources);

private:
    void SnapshotGCCallback(Snapshot::Ptr ss_ptr);
    Snapshots() {
        Init();
    }
    void Init();

    mutable std::shared_timed_mutex mutex_;
    SnapshotHolderPtr LoadNoLock(ID_TYPE collection_id);
    SnapshotHolderPtr Load(ID_TYPE collection_id);
    SnapshotHolderPtr GetHolderNoLock(ID_TYPE collection_id);

    std::map<ID_TYPE, SnapshotHolderPtr> holders_;
    std::map<std::string, ID_TYPE> name_id_map_;
    std::vector<Snapshot::Ptr> to_release_;
};
