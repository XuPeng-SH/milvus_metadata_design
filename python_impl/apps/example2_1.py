import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s | %(levelname)s | %(message)s (%(filename)s:%(lineno)d)')

import sys
import os
if __name__ == '__main__':
    sys.path.append(os.path.dirname(os.path.dirname(
        os.path.abspath(__file__))))

import pdb
from apps.example2 import *
from database.factories import DEL_FILE
from utils import get_lsn

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


# Get latest snapshot
used_snapshot = ss_mgr.get(c1.id)

existing_segment = c1.segments.first()
logger.info(f'Adding new del file to segment {existing_segment.id}')
del_f = existing_segment.create_file(ftype=DEL_FILE, lsn=get_lsn(), size=10000, version={'server': '0.9.0'})

pdb.set_trace()
new_ss = create_collection_commit(c1, segment=existing_segment, new_files=[del_f])
Commit(new_ss)
ss_mgr.append(new_ss)

pdb.set_trace()
logger.info(f'{ss_mgr.active_snapshots(c1)}')

ss_mgr.release(used_snapshot)
pdb.set_trace()
logger.info(f'{ss_mgr.active_snapshots(c1)}')

used_snapshot = ss_mgr.get(c1)
logger.info(used_snapshot.commits.all())
