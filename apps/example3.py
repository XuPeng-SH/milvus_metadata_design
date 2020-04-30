import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

from apps.managers import (CollectionsMgr, PartitionCommitsMgr, SegmentsMgr, SegmentFilesMgr, SegmentsCommitsMgr, db,
        Collections, FieldsMgr, FieldElementsMgr)


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s (%(filename)s:%(lineno)d)')

from apps.example2 import *

from database.models import FieldCommits, SchemaCommits
# from database.utils import Commit

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Init databases
# db.drop_all()
# db.create_all()

vf_sq8 = vf.add_element(name='sq8', ftype=IVFSQ8_INDEX, params={'metric_type': 'IP'})
Commit(vf_sq8)

vf_new_field_commit = FieldCommits(field=vf, mappings=[vf_sq8.id] + vf_commit.mappings, collection=c1)
Commit(vf_new_field_commit)

new_schema = SchemaCommits(collection=c1, mappings=[vf_new_field_commit.id] +
    schema1.mappings)
Commit(new_schema)

new_ss = create_collection_commit(c1)
Commit(new_ss)
