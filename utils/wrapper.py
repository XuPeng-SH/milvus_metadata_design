import logging
from utils.events import ReferenceType, ResourceReferenceEvent, DBEvent, DBAction

logger = logging.getLogger(__name__)


class BaseWrapper:
    def __getattr__(self, attr):
        if attr in self.__dict__:
            return self.__dict__[attr]
        return getattr(self.node, attr)


class Wrapper(BaseWrapper):
    def __init__(self, node, cleanup=None, event_handler=None):
        self.node = node
        self.prev = None
        self.next = None
        self.refcnt = 0
        self.cleanupcb = []
        self.event_handler = event_handler
        cleanup and self.cleanupcb.append(cleanup)

    def set_next(self, next_node):
        next_node.prev = self
        self.next = next_node
        if self.refcnt == 0:
            self.cleanup()
        return self

    def set_prev(self, prev_node):
        prev_node.next = self
        self.prev = prev_node

    def is_tail(self):
        return self.next == None

    def is_head(self):
        return self.prev == None

    def ref(self, context=None):
        self.refcnt += 1
        logger.info(f'{context.log() if context else ""} | Ref   {self.node.__class__.__name__}:{self.id} CNT={self.refcnt}')
        event = ResourceReferenceEvent(ReferenceType.REF, self.refcnt,
                resource=f'{self.node.__class__.__name__}:{self.node.id}')
        self.event_handler and self.event_handler.handle(event)

    def unref(self, context=None):
        self.refcnt -= 1
        logger.info(f'{context.log() if context else ""} | Unref {self.node.__class__.__name__}:{self.id} CNT={self.refcnt}')
        event = ResourceReferenceEvent(ReferenceType.UNREF, self.refcnt,
                resource=f'{self.node.__class__.__name__}:{self.node.id}')
        self.event_handler and self.event_handler.handle(event)
        if self.refcnt <= 0:
            self.cleanup()

    def register_cb(self, cb):
        self.cleanupcb.append(cb)

    def cleanup(self):
        self.do_cleanup()
        for cb in self.cleanupcb:
            cb(self.node)

    def do_cleanup(self):
        pass


class DBInstanceWrapper(Wrapper):
    def __init__(self, node, db, cleanup=None, **kwargs):
        super().__init__(node, cleanup=cleanup, **kwargs)
        self.model = node.__class__
        self.db = db

    def do_cleanup(self):
        self.db.Session.delete(self.node)
        self.db.Session.commit()
        event = DBEvent(DBAction.DELETE, resource=f'{self.node.__tablename__}:{self.node.id}')
        self.event_handler and self.event_handler.handle(event)


class ScopedWrapper(BaseWrapper):
    def __init__(self, node):
        self.node = node
        self.node.ref()

    def __del__(self):
        self.node.unref()


# class HirachyDBProxy(DBInstanceWrapper):
#     def __init__(self, node, parent, cleanup=None):
#         super().__init__(node, cleanup=cleanup)
#         self.parent = parent
#         self.parent.ref()

#     def do_cleanup(self):
#         super().do_cleanup()
#         self.parent.unref()
