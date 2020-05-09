import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

import logging
import faker
from database.models import db, Fields, FieldCommits, SchemaCommits, Collections, LogSequenceNumbers
from database.factories import create_collection_commit
from database.utils import Commit
from utils.events import HistoryLogHandler, SortBy
from utils import get_lsn

from utils.managers import DBDataManager

FAKER = faker.Faker()

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s (%(filename)s:%(lineno)d)')

# Define some const variables
VECTOR_FIELD = 1
INT_FIELD = 2
STRING_FIELD = 3

RAW = 1
IVFSQ8_INDEX = 100
IVFFLAT_INDEX = 101
PQ_INDEX = 102

event_handler = HistoryLogHandler()

# Init databases
# db.drop_all()
db.create_all()

# Init mgrs
data_manager = DBDataManager(db=db, event_handler=event_handler, snapshots=1)

collection_params = {
    'name': FAKER.word(),
    'fields': [
        {
            'name': 'vector',
            'ftype': VECTOR_FIELD,
            'params': {'dimension': 512},
            'elements': [
                {
                    'name': 'pq',
                    'ftype': PQ_INDEX,
                    'params': {'metric_type': 'IP'}
                }
            ]
        },
        {
            'name': 'id',
            'ftype': STRING_FIELD,
        }
    ]
}

c1 = data_manager.create_collection(collection_params)

# Create a snapshot
s1 = create_collection_commit(data_manager, c1, lsn=get_lsn())

# Get latest snapshot
latest = data_manager.get_snapshot(c1.id)

# # Create a snapshot
s2 = create_collection_commit(data_manager, c1, lsn=get_lsn())

data_manager.release_snapshot(latest)


schema = data_manager.get_schema(c1.id)
data_manager.release_schema(schema)

new_partition = data_manager.create_partition(c1.id, 'p2')

# # Drop s2. c1 also will be deleted
# data_manager.collection_commit_mgr.drop(c1)
# lsn_entry = LogSequenceNumbers(lsn=get_lsn())
# Commit(lsn_entry)
# event_handler.dump(sort_key=SortBy.TS)
