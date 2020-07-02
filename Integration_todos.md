# Integration

## WalManager -- C
- [ ] Add New **Init**
> WalManager could init from Snapshot
- [ ] Use ID as primary key
```cpp
ErrorCode
Init(snapshot::ScopedSnapshot ss);
```

## MemManagerImpl -- A

- [ ] Replace meta_ with snapshot

## MemTable -- A

- [ ] Replace meta_ with snapshot

## MemTableFile -- A

- [ ] Replace meta_ with snapshot

## DBImpl

- [ ] Snapshot action implementation in **Start()** -- **C**
- [ ] Snapshot action implementation in **Stop()** -- **C**
- [ ] **DropAll()** -- **B**
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
- [ ] **UpdateCollectionFlag** -- **D**
- [ ] **GetCollectionRowCount** -- **B**
- [ ] **GetCollectionInfo** -- **B**
- [ ] **ReLoadSegmentsDeletedDocs** -- **A**
- [ ] **PreloadCollection** -- **B**
    - [x] DBImpl bussiness logic
    - [ ] [LoadVectorFieldHandler](##SnapshotHandlers###LoadVectorFieldHandler)
- [ ] **Compact** -- **A**
- [ ] **CompactFile** -- **A**
- [ ] **GetVectorsByID** -- **C**
- [ ] **GetVectorsByIDHelper** -- **C**
- [ ] **GetEntitiesByID** -- **C**
- [ ] **GetEntitiesByIDHelper** -- **C**
- [ ] **CreateIndex** -- **A**
- [ ] **SerializeStructuredIndex** -- **D**
- [ ] **GetVectorIDs** -- **C**
- [ ] **GetVectorIDs** -- **C**
- [ ] **FlushAttrsIndex** -- **D**
- [ ] **DescribeIndex** -- **D**
- [ ] **DropIndex** -- **B**
> Need new CompoundOperation
- [ ] **Query** -- **C**
- [ ] **QueryByFileID** -- **C**
- [ ] **Size** -- **B**
- [ ] **BackgroundMerge** -- **A**
    - [x] DBImpl bussiness logic
    - [ ] [MergeManagerImpl][##MergeMangerImpl]
- [ ] **BackgroundBuildIndex** -- **A**
- [ ] **GetPartitionByTag** -- **C**
- [ ] **GetPartitionByTags** -- **C**
- [ ] **ExecWalRecord** -- **C**

- [ ] **InsertVectors**: `?` -- **C**
- [ ] **InsertEntities**: `?` -- **C**
- [ ] **CopyToAttr**: `?` -- **C**
- [ ] **DeleteVector**: `?` -- **C**
- [ ] **DeleteVectors**: `?` -- **C**
- [ ] **Flush**: `?` -- **C**
- [ ] **CreateStructuredIndex**: `?` -- **D**
- [ ] **QueryByIDs**: `?` -- **C**
- [ ] **QueryAsync**: `?` -- **C**
- [ ] **GetFilesToBuildIndex**: `?` -- **D**

## MergeMangerImpl -- A
> Replace meta_ptr_ with snapshot

## SnapshotHandlers -- B

### LoadVectorFieldHandler -- B
- [ ] Bussiness logic
- [ ] [LoadVectorFieldElementHandler](###LoadVectorFieldElementHandler)

### LoadVectorFieldElementHandler -- B
- [ ] Bussiness logic
> Need to be discussed
