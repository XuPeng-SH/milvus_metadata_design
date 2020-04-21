import random
from init_db import db

SQLALCHEMY_DATABASE_URI='sqlite:////tmp/meta_lab/meta.sqlite?check_same_thread=False'
db.init_db(uri=SQLALCHEMY_DATABASE_URI)

import models

db.drop_all()
db.create_all()

from factories import (CollectionsFactory, CollectionFieldsFactory, CollectionFieldIndiceFactory,
        SnapshotsFactory)

collection = CollectionsFactory()
collection.version = {'one': '1'}
collection.save()

field0 = CollectionFieldsFactory(num=0, collection=collection)
field1 = CollectionFieldsFactory(num=1, collection=collection)

fields = collection.fields.all()

for field in fields:
    CollectionFieldIndiceFactory.create_batch(random.randint(1,4) ,field=field)
    print(field.num, field.name, field.ftype)
    indice = field.indice.all()
    for index in indice:
        print(f'\t\t{index.name} {index.ftype}')

SnapshotsFactory.create_batch(random.randint(2,6), collection=collection)

snapshots = collection.snapshots.all()
for snapshot in snapshots:
    print(f'Snapshot {snapshot.id} {snapshot.collection.name}')
