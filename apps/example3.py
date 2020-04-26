import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

from apps.managers import (CollectionsMgr, SnapshotsMgr, SegmentsMgr, SegmentFilesMgr, SegmentsCommitsMgr, db,
        Collections, FieldsMgr, FieldElementsMgr)


import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s (%(filename)s:%(lineno)d)')

from apps.example2 import *

from database.models import FieldCommits
# from database.utils import Commit

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Init databases
# db.drop_all()
# db.create_all()

# c2 = Collections(name=FAKER.word())
# vf = c1.create_field(name='vector', ftype=VECTOR_FIELD, params={'dimension': 512})
# vfi = vf.add_element(name='sq8', ftype=IVFSQ8, params={'metric_type': 'L2'})
# vfc = FieldCommits(collection=c2, field=vf, mappings=)

# idf = c1.create_field(name='id', ftype=STRING_FIELD)
# Commit(c1, vf, vfi, idf)
# logger.debug(f'Fields: {[ (f.name, f.params) for f in c1.fields.all()]}')



field = c1.fields.first()
field_elements = field.elements.all()

field_commit = FieldCommits(collection=c1, field=field,
        mappings=list(map(lambda element: element.id, field_elements)))
Commit(field_commit)

seg = c1.segments.first()
seg_file = seg.files.first()
print(f'{seg_file} {seg_file.id} {seg_file.field} {seg_file.field_element}')
