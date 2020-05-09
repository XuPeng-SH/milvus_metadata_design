import enum
import logging
import datetime
import threading
from collections import defaultdict

logger = logging.getLogger(__name__)

class NoopHandler:
    def handle(self, event):
        pass

class SortBy(enum.Enum):
    TS = 1
    TYPE = 2

class HistoryLogHandler:
    cv = threading.Condition()
    events = defaultdict(list)
    def handle(self, event):
        with self.cv:
            self.events[event.__class__].append(event)

    def dump_one_no_lock(self, event_type, start_idx=0, clear=True):
        if clear:
            events = self.events.pop(event_type, None)
        else:
            events = self.events.get(event_type, None)
        if not events:
            return
        logger.info(f'{event_type.__name__}')
        gidx = 0
        for idx, event in enumerate(events):
            gidx = idx + start_idx
            logger.info(f'{gidx}. {event.message}')

        return gidx


    def dump(self, event_type=None, sort_key=SortBy.TYPE, clear=True):
        with self.cv:
            if event_type:
                self.dump_one_no_lock(event_type, clear=clear)
                return

            if sort_key == SortBy.TYPE:
                gidx = 0
                for event_type, _ in self.events.items():
                    gidx = self.dump_one_no_lock(event_type, start_idx=gidx, clear=False)
                clear and self.events.clear()
                return

            events = []
            for _, es in self.events.items():
                events.extend(es)

            events = sorted(events, key=lambda event: event.ts)

            for idx, event in enumerate(events):
                logger.info(f'{idx}. {event.message}')

            clear and self.events.clear()


class ReferenceType(enum.Enum):
    REF = 1
    UNREF = 2
    EMPTY = 3

class ResourceReferenceEvent:
    def __init__(self, action_type, reference_cnt, resource, **kwargs):
        self.action_type = action_type
        self.resource = resource
        self.reference_cnt = reference_cnt
        self.kwargs = kwargs
        self.ts = datetime.datetime.now().timestamp()

    @property
    def message(self):
        return f'{self.action_type} ({self.resource}, refcnt={self.reference_cnt}, {self.ts})'


class DBAction(enum.Enum):
    ADD = 1
    DELETE = 2
    UPDATE = 3

class DBEvent:
    def __init__(self, action, resource, **kwargs):
        self.action = action
        self.resource = resource
        self.kwargs = kwargs
        self.ts = datetime.datetime.now().timestamp()

    @property
    def message(self):
        return f'{self.action} ({self.resource}, {self.ts})'


class UpdateAction(enum.Enum):
    ADD = 1
    DELELTE = 2

class UpdateLevel2ResourceEvent:
    def __init__(self, action, level1_id, level2_id, resource, repo_name, **kwargs):
        self.action = action
        self.resource = resource
        self.repo_name = repo_name
        self.level1_id = level1_id
        self.level2_id = level2_id
        self.kwargs = kwargs
        self.ts = datetime.datetime.now().timestamp()

    @property
    def message(self):
        return f'{self.action} ({self.level1_id}, {self.level2_id}, {self.repo_name}, {self.ts})'

    def __str__(self):
        ret = f'TS={self.ts} {self.__class__.__name__}:\n '
        ret += f'\t{self.action} L1={self.level1_id} L2={self.level2_id}\n'
        ret += f'\tR={self.resource} REPO={self.repo_name}'
        return ret
