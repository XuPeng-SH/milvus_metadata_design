import logging
import enum
import threading

logger = logging.getLogger(__name__)


class HubProxyStatus(enum.Enum):
    FOLLOW_DUPLICATED = 1
    FOLLOW_OK         = 2

    UNFOLLOW_NON_EXISTED = 101
    UNFOLLOW_OK = 102


class ListnerHubMixin:
    def __init__(self, pre_follow_cbs=None, pre_unfollow_cbs=None,
            post_follow_cbs=None, post_unfollow_cbs=None):
        self.listners = {}
        self.pre_follow_cbs = [] if not pre_follow_cbs else pre_follow_cbs
        self.pre_unfollow_cbs = [] if not pre_unfollow_cbs else pre_unfollow_cbs
        self.post_follow_cbs = [] if not post_follow_cbs else post_follow_cbs
        self.post_unfollow_cbs = [] if not post_unfollow_cbs else post_unfollow_cbs
        self.cv = threading.Condition()

    def register_pre_follow_cb(self, cb):
        assert callable(cb)
        with self.cv:
            self.pre_follow_cbs.append(cb)

    def register_post_follow_cb(self, cb):
        assert callable(cb)
        with self.cv:
            self.post_follow_cbs.append(cb)

    def register_pre_unfollow_cb(self, cb):
        assert callable(cb)
        with self.cv:
            self.pre_unfollow_cbs.append(cb)

    def register_post_unfollow_cb(self, cb):
        assert callable(cb)
        with self.cv:
            self.post_unfollow_cbs.append(cb)

    def get_listner_key(self, listner, **kwargs):
        return listner.key()

    def on_duplicate_listner(self, listner, **kwargs):
        return HubProxyStatus.FOLLOW_DUPLICATED

    def on_new_listener(self, key, listner, **kwargs):
        with self.cv:
            self.listners[key] = listner
        return HubProxyStatus.FOLLOW_OK

    def on_unfollow_non_existed_listner(self, k, listner, **kwargs):
        status = HubProxyStatus.UNFOLLOW_NON_EXISTED
        logger.error(f'{str(status)} {k}')
        return status

    def on_unfollow_listner(self, k, listner, **kwargs):
        with self.cv:
            self.listner.pop(k)
        return HubProxyStatus.UNFOLLOW_OK

    def add_listner(self, listner, **kwargs):
        k = self.get_listner_key(listner, **kwargs)
        if k in self.listners:
            return self.on_duplicate_listner(listner, **kwargs)

        for cb in self.pre_follow_cbs:
            cb(self, listner, **kwargs)

        ret = self.on_new_listener(k, listner, **kwargs)

        for cb in self.post_follow_cbs:
            cb(self, listner, result=ret, **kwargs)

        return ret

    def remove_listner(self, listner, **kwargs):
        k = self.get_listner_key(listner, **kwargs)
        if k not in self.listners:
            return self.on_unfollow_non_existed_listner(k, listner, **kwargs)

        for cb in self.pre_unfollow_cbs:
            cb(self, listner, **kwargs)

        ret = self.on_unfollow_listner(k, listner, **kwargs)

        for cb in self.post_unfollow_cbs:
            cb(self, listner, result=ret, **kwargs)

        return ret

if __name__ == '__main__':
    def ff(hub, listner, **kwargs):
        logger.error(f'PRE {hub} {listner}')

    def xx(hub, listner, result=None, **kwargs):
        logger.error(f'POST {result} {hub} {listner}')

    class MyListner:
        def __init__(self, key):
            self._key = key

        def key(self):
            return self._key

    hub = ListnerHubMixin()
    l1 = MyListner(1)
    l2 = MyListner(2)
    hub.register_pre_follow_cb(ff)
    hub.register_post_follow_cb(xx)

    hub.add_listner(l1)

    hub.remove_listner(l2)
