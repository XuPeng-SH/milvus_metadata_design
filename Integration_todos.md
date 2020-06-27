# Integration

## WalManager
- [ ] Add New **Init**
> WalManager could init from Snapshot
- [ ] Use ID as primary key
```cpp
ErrorCode
Init(snapshot::ScopedSnapshot ss);
```

## MemManagerImpl

- [ ] Replace meta_ with snapshot

## MemTable

- [ ] Replace meta_ with snapshot

## MemTableFile

- [ ] Replace meta_ with snapshot

## DBImpl

- [ ] Snapshot action implementation in **Start()**
- [ ] Snapshot action implementation in **Stop()**
- [ ] **DropAll()**
- [x] **CreateCollection**
- [x] **DropCollection**
- [x] **DescribeCollection**
- [x] **HasCollection**
- [x] **AllCollections**
- [x] **CreatePartition**
- [x] **HasPartition**
- [x] **DropPartition**
> EraseMemVector Needed?
- [x] **ShowPartitions**
- [ ] **UpdateCollectionFlag**
- [ ] **GetCollectionRowCount**
- [ ] **GetCollectionInfo**
- [ ] **ReLoadSegmentsDeletedDocs**
- [ ] **PreloadCollection**
    - [x] DBImpl bussiness logic
    - [ ] [LoadVectorFieldHandler](##SnapshotHandlers###LoadVectorFieldHandler)
- [ ] **Compact**
- [ ] **CompactFile**
- [ ] **GetVectorsByID**
- [ ] **GetVectorsByIDHelper**
- [ ] **GetEntitiesByID**
- [ ] **GetEntitiesByIDHelper**
- [ ] **CreateIndex**
- [ ] **SerializeStructuredIndex**
- [ ] **GetVectorIDs**
- [ ] **GetVectorIDs**
- [ ] **FlushAttrsIndex**
- [ ] **DescribeIndex**
- [ ] **DropIndex**
> Need new CompoundOperation
- [ ] **Query**
- [ ] **QueryByFileID**
- [ ] **Size**
- [ ] **BackgroundMerge**
    - [x] DBImpl bussiness logic
    - [ ] [MergeManagerImpl][##MergeMangerImpl]
- [ ] **BackgroundBuildIndex**
- [ ] **GetPartitionByTag**
- [ ] **GetPartitionByTags**
- [ ] **ExecWalRecord**

- [ ] **InsertVectors**: `?`
- [ ] **InsertEntities**: `?`
- [ ] **CopyToAttr**: `?`
- [ ] **DeleteVector**: `?`
- [ ] **DeleteVectors**: `?`
- [ ] **Flush**: `?`
- [ ] **CreateStructuredIndex**: `?`
- [ ] **QueryByIDs**: `?`
- [ ] **QueryAsync**: `?`
- [ ] **GetFilesToBuildIndex**: `?`
-
## MergeMangerImpl
> Replace meta_ptr_ with snapshot

## SnapshotHandlers

### LoadVectorFieldHandler
- [ ] Bussiness logic
- [ ] [LoadVectorFieldElementHandler](###LoadVectorFieldElementHandler)

### LoadVectorFieldElementHandler
- [ ] Bussiness logic
> Need to be discussed
