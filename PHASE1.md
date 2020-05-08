


# Milvus 元数据修改Phase I - 替换已有接口

## 接口

###  CreateCollection
>**函数原型**
```cpp
Status CreateCollection(CollectionSchema& collection_schema);
```
>**参数 CollectionSchema**
```json
{
    "name": "collection_1",
    "fields": [
        {
            "name": "vector",
            "ftype": "VECTOR_FIELD",
            "params": {"dimension": 512},
            "elements": [
                {
                    "name": "pq",
                    "ftype": "PQ_INDEX",
                    "params": {"metric_type": "IP"}
                }
            ]
        },
        {
            "name": "id",
            "ftype": "STRING_FIELD"
        }
    ]
}
```
>**内部实现**
- 创建 Collection
	```python
	all_records = []
	collection_name = collection_schema['name']
	collection = DBCreateCollection(name=collection_name)
	DBCommit(collection)
	```
- 创建 Fields, FieldElements, FieldCommits, SchemaCommits
	```python
	collection_id = collection.id
	field_commits = []
	field_schemas = collection_schema['fields']
	for field_id, field_schema in enumerate(field_schemas):
		field_name = field_schema['name']
		field_type = field_schema['ftype']
		field_params = field_schema['params']
		field = DBCreateFields(name=field_name, num=field_id,
                               params=field_params, collection_id=collection_id)
		DBCommit(field)
		all_records.append(field)
		field_elements = []
		field_element = DBCreateFieldElements(name='_RAW', ftype=RAW_ELEMENT, field_id=field.id)
		field_elements.append(field_element)
		element_schemas = field_schema['elements']
		for element_id, element_schema in element_schemas:
			element_name = element_schema['name']
			element_type = element_schema['ftype']
			element_params = element_schema['params']
			element = DBCreateFieldElements(name=element_name, ftype=element_type,
                                            params=element_params, field_id=field.id)
			field_elements.append(element)
			all_records.append(element)

		DBCommit(**field_elements)
		field_commit = DBCreateFieldCommits(collection_id=collection_id,
                                            mappings=[ele.id for ele in field_elements],
                                            field_id=field.id)
		DBCommit(field_commit)
		field_commits.append(field_commit)
		all_records.append(field_commit)
	schema_commit = DBCreateSchemaCommits(collection_id=collection_id,
                                          mappings=[ele.id for ele in field_commits]
	DBCommit(schema_commit)
	all_records.append(schema_commit)
	```
- 创建 Default Partition, PartitionCommits
	```python
	partition = DBCreatePartitions(collection_id=collection_id, name='_default')
	DBCommit(partition)
	partition_commit = DBCreatePartitionCommits(collection_id=collection_id,
                                                mappings=[], partition_id=partition.id)
	DBCommit(partition_commit)
	all_records.append(partition)
	all_records.append(partition_commit)
	```
- 创建 CollectionCommit 并最后提交
	```python
	collection_commits = DBCreateCollectionCommits(collection_id=collection_id,
                                                   schema_commit_id=schema_commit.id,
                                                   mappings=[partition_commit.id])
	for record in all_records:
		record.status = ACTIVE
	all_records.append(collection_commits)
	DBCommit(*all_records)
	```
- 将新的 Collection 提交给内存数据管理器
	```python
	data_manager.submit(collection)
	```

### **DescribeCollection**
>**函数原型**
```cpp
Status DescribeCollection(CollectionSchema& collection_schema);
```
>**内部实现**
- 获取并冻结最新的 CollectionCommits
	```python
	collection_name = collection_schema['name']
	collection_commit = data_manger.get_collection_commit(name=collection_name)
	```
- 通过 CollectionCommits 获取 schema
	```python
	field_schemas = collection_commit.field_schemas.to_json()
	collection_schema['fields'] = field_schemas
	```
- 释放冻结的 CollectionCommits
	```python
	data_manager.release(collection_commit)
	```
### **HasCollection**
>**函数原型**
```cpp
Status HasCollection(const std::string& name, bool& has_or_not);
```
>**内部实现**
- 通过数据管理器根据 collection 名字查询指定 collection 是否存在
	```python
    has_or_not = data_manager.has_collection(collection_name)
	```
### **DropCollection**
>**函数原型**
```cpp
Status DropCollection(const std::string& name);
```
>**内部实现**
- 获取 CollectionCommits 管理器，并调用 drop
	```python
    collection_commits_mgr.drop(name)
	```
>**上层逻辑影响**
- DBImpl::DropCollection
  只需要调用元数据接口 DropCollection

### **DeleteCollectionFiles**
>**函数原型**
```cpp
Status DeleteCollectionFiles(const std::string& name);
```
>**Deprecated**

这个接口是之前元数据GC的一部分，之后元数据的GC由数据管理器接管

### **CreateCollectionFile**
>**函数原型**
```cpp
Status CreateCollectionFile(SegmentSchema& file_schema);
```
>**内部实现**
- 调用数据管理器获取最新的 Schema
	```python
    partition_id = file_schema.partition_id
    collection_id = file_schema.collection_id
    prev_collection_commit = data_manager.get_collection_commit(name)
    schema = prev_collection_commit.schema
    element_schema = prev_collection_commit.field_element_schema(field_name, element_name)
    ```
- 是否创建新的 Segment
    ```python
    all_records = []
    segment_id = file_schema.segment_id
    new_segment = False
    if not segment_id:
        segment = DBCreateSegment(partition_id=partition_id)
        DBCommit(segment)
        all_records.append(segment)
        segment_id = segment.id
        new_segment = True

    ```
- 创建 SegmentFile, SegmentCommit, PartitionCommit, CollectionCommit
    ```python
    segment_file = DBCreateSegmentFile(field_element_id=element_schema.id, partition_id=partition_id,
                                       segment_id=segment_id, **kwargs)
    DBCommit(segment_file)
    all_records.append(segment_file)

    mappings = [segment_file.id]
    prev_segment_commit_id = None
    if not new_segment:
        prev_segment_commit = prev_collection_commit.segment_commit(segment_id)
        mappings.extend(prev_segment_commit.mappings)
        prev_segment_commit_id = prev_segment_commit.id
    segment_commit = DBCreateSegmentCommits(partition_id=partition_id, segment_id=segment_id,
                                             schema_id=schema.id, mappings=mappings)
    DBCommit(segment_commit)
    all_records.append(segment_commit)
    prev_partition_commit = prev_collection_commit.partition_commit(partition_id)
    mappings = prev_partition_commit.mappings
    if prev_segment_commit_id is not None:
        mappings.remove(prev_segment_commit_id)
    mappings.append(segment_commit.id)
    partition_commit = DBCreatePartitionCommits(partition_id=partition_id, collection_id=collection_id,
                                                mappings=mappings)
    DBCommit(partition_commit)
    all_records.append(partition_commit)

    mappings = prev_collection_commit.mappings
    mappings.remove(prev_partition_commit.id)
    mappings.append(partition_commit.id)
    collection_commit = DBCreateCollectionCommits(collection_id=collection_id, mappings=mappings)
    for record in all_record:
        record.status = ACTIVE
    all_records.append(collection_commit)
    DBCommit(*all_records)
    data_manager.submit(collection_commit)
    ```

- 释放之前锁定资源
    ```python
    data_manager.release(prev_collection_commit)
    ```
>**上层逻辑影响**

应该调用元数据 MergeFiles 接口
- DBImpl::MergeFiles
- DBImpl::CompactFile

### **UpdateCollectionFile** **UpdateCollectionFiles**
>**函数原型**
```cpp
Status UpdateCollectionFile(SegmentSchema& file_schema);
Status UpdateCollectionFiles(SegmentsSchema& files_schema);
```
>**Deprecated**

之后元数据有版本，没有必要修改
