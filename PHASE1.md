# Milvus 元数据修改Phase I - 替换已有接口

## 接口
>**函数原型**
```cpp
Status CreateCollection(CollectionSchema& collection_schema);
```
>**CollectionSchema**
```json
{
    "name": "collection_1",
    "fields": [
        {
            "name": "vector",
            "ftype": VECTOR_FIELD,
            "params": {"dimension": 512},
            "elements": [
                {
                    "name": "pq",
                    "ftype": PQ_INDEX,
                    "params": {"metric_type": "IP"}
                }
            ]
        },
        {
            "name": "id",
            "ftype": STRING_FIELD
        }
    ]
}
```
