import logging
from collections import defaultdict, OrderedDict
from functools import partial

from utils.wrapper import DBInstanceWrapper, ScopedWrapper
from utils.events import (UpdateLevel2ResourceEvent, UpdateAction, NoopHandler)
from utils import ReferenceContext

logger = logging.getLogger(__name__)


def BinUnrefFirstCB(first, context, second):
    if first is None:
        import ipdb;ipdb.set_trace()
    first.unref(context)


class Level1ResourceMgr:
    level_one_model = None
    wrapper_class = DBInstanceWrapper
    pk = 'id'
    def __init__(self, db, notify_listners=None, event_handler=None):
        self.context = ReferenceContext(f'{self.__class__.__name__}')
        self.db = db
        self.event_handler = event_handler
        self.resources = OrderedDict()
        self.load_listners = notify_listners if notify_listners else []
        self.load_all()

    def cleanupcb(self, target):
        self.resources.pop(target.id, None)

    def get_record(self, level1_id, context=None):
        record = self.db.Session.query(self.level_one_model).filter(getattr(self.level_one_model,
            self.pk)==level1_id).first()
        return record

    def get_all_records(self, context=None):
        records = self.db.Session.query(self.level_one_model).all()
        return records

    def wrap_record(self, record, context=None):
        wrapped = self.wrapper_class(record, cleanup=self.cleanupcb, db=self.db,
                event_handler=self.event_handler)
        return wrapped

    def load_all(self, context=None):
        records = self.get_all_records(context=context)
        for record in records:
            self.load(record.id, context=context)

    def register_load_listners(self, *listners, context=None):
        self.load_listners.extend(listners)

    def notify_load_listners(self, wrapped, context=None):
        for listner in self.load_listners:
            listner(wrapped)

    def load(self, level1_id, context=None):
        context = context if context else self.context
        if self.resources.get(level1_id, None):
            return self.resources[level1_id]
        record = self.get_record(level1_id, context=context)
        wrapped = self.wrap_record(record, context=context)
        self.notify_load_listners(wrapped, context=context)
        self.resources[level1_id] = wrapped
        return wrapped

    def get(self, level1_id, context=None):
        record = self.resources.get(level1_id, None)
        if not record:
            record = self.load(level1_id, context=context)
        context = context if context else self.context
        # context.add_tag('GET')
        record and record.ref(context.clone().add_tag('GET'))
        # record = ScopedWrapper(record) if record else record
        return record

    def release(self, record, context=None):
        context = context if context else self.context
        # context.add_tag('RELEASE')
        record and record.unref(context.clone().add_tag('RELEASE'))

    def dump(self):
        logger.info(f'****** Start Dumping Resources: {self.__class__.__name__} ******')
        level1_attr_name = self.level_one_model.__tablename__
        for level1_id, level1_resource in self.resources.items():
            logger.info(f'[ {level1_attr_name}:{level1_id} RefCnt:{level1_resource.refcnt}]')
            # for _, resource in level1_resources.items():
            #     logger.info(f'  ( {level2_attr_name}:{resource.id} RefCnt:{resource.refcnt} )')
        logger.info(f'*****************************************************')
        # logger.info(f'****** End   Dumping Resources: {self.__class__.__name__} ******\n')



#     level2_model = None
#     level3_model = None
#     link1_key = ''
#     link2_key = ''
#     wrapper_class = DBInstanceWrapper
#     pk = 'id'

#     def __init__(self, db, wrapper_kwargs=None, event_handler=None):
#         self.db = db
#         self.event_handler = event_handler if event_handler else NoopHandler()
#         self.wrapper_kwargs = wrapper_kwargs if wrapper_kwargs is not None else {'db': self.db}
#         self.resources = defaultdict(defaultdict(OrderedDict))
#         self.load_all()

#     def load_all(self):
#         level1_id_generator = self.get_level1_id_generator()
#         for level1_id in level1_id_generator:
#             self.load(level1_id)

#     def load(self, level1_id, level2_id=None, level3_id=None):
#         level1_resources = self.resources.get(level1_id, None)
#         if level1_resources is None:
#             if level2_id is None:
#                 self.load_all_level2_level3_resources(level1_id)
#             else:
#                 if level3_id is None:
#                     self.load_all_level3_resources(level1_id, level2_id)
#                 else:
#                     self.load_level3_resources(level1_id, level2_id, level3_id)
#             return

#         if level2_id is None:
#             return

#         level2_resources = level1_resources.get(level1_id, None)
#         if level2_resources is None:
#             if level3_id is None:
#                 self.load_all_level3_resources(level1_id, level2_id)
#             else:
#                 self.load_level3_resources(level1_id, level2_id, level3_id)

#             return

#         if level3_id is None:
#             return

#         level3_resources = level3_resources.get(level3_id, None)
#         if level3_resources is not None:
#             return

#         self.load_level3_resources(level1_id, level2_id, level3_id)

#     def load_all_level2_level3_resources(self, level1_id):
#         query = self.db.Session.query(self.level2_model).filter(getattr(self.level2_model,
#             self.link1_key)==level1_id)

class Level2ResourceMgr:
    level_one_model = None
    level_two_model = None
    link_key = ''
    wrapper_class = DBInstanceWrapper
    pk = 'id'
    def __init__(self, db, wrapper_kwargs=None, event_handler=None):
        self.context = ReferenceContext(f'{self.__class__.__name__}')
        self.event_handler = event_handler if event_handler else NoopHandler()
        self.db = db
        self.wrapper_kwargs = wrapper_kwargs if wrapper_kwargs else {'db': self.db}
        self.resources = defaultdict(OrderedDict)
        self.load_all()

    def add_resource(self, level1_id, level2_id, resource):
        self.resources[level1_id][level2_id] = resource

    def remove_resource(self, level1_id, level2_id=None):
        if level2_id is None:
            return self.resources.pop(level1_id, None)
        level1_resources =  self.resources.get(level1_id, None)
        if level1_resources is None:
            return None

        return level1_resources.pop(level2_id, None)

    @property
    def level1_ids(self):
        return list(self.resources.keys())

    def dump(self):
        logger.info(f'****** Start Dumping Resources: {self.__class__.__name__} ******')
        level1_attr_name = self.level_one_model.__tablename__
        level2_attr_name = self.level_two_model.__tablename__
        for level1_id, level1_resources in self.resources.items():
            logger.info(f'[ {level1_attr_name}:{level1_id} ]')
            for _, resource in level1_resources.items():
                logger.info(f'  ( {level2_attr_name}:{resource.id} RefCnt:{resource.refcnt} )')
        logger.info(f'*****************************************************')
        # logger.info(f'****** End   Dumping Resources: {self.__class__.__name__} ******\n')

    def cleanupcb(self, target):
        level_one_key = getattr(target, self.link_key)
        logger.debug(f'ResourceMgr Removing l1={level_one_key} l2={target.id}')
        # resource = self.resources[level_one_key].pop(target.id, None)
        resource = self.remove_resource(level_one_key, target.id)
        if resource:
            event = UpdateLevel2ResourceEvent(action=UpdateAction.DELELTE, resource=f'{resource.node.__class__.__name__}:{resource.id}',
                    level1_id=level_one_key, level2_id=target.id, repo_name=self.__class__.__name__)
            self.event_handler.handle(event)
        if len(self.resources[level_one_key]) == 0:
            # self.resources.pop(level_one_key)
            self.remove_resource(level_one_key)

    def load_level2_resources(self, level1_id, context=None, **kwargs):
        query = self.db.Session.query(self.level_two_model).filter(getattr(self.level_two_model,
            self.link_key)==level1_id)
        level2_id = kwargs.pop('level2_id', None)
        if level2_id is not None:
            query = query.filter(getattr(self.level_two_model, self.pk)==level2_id)
        records = query.all()
        return records

    def update_level2_resource(self, level1_id, level2_id, record, context=None):
        wrapper = self.wrapper_class(record, cleanup=self.cleanupcb,
                event_handler=self.event_handler, **self.wrapper_kwargs)
        return wrapper

    def update_resources(self, level1_id, level2_id, resource, context=None):
        # self.resources[level1_id][level2_id] = resource
        self.add_resource(level1_id, level2_id, resource)
        event = UpdateLevel2ResourceEvent(action=UpdateAction.ADD, resource=f'{resource.node.__class__.__name__}:{resource.id}',
                level1_id=level1_id, level2_id=level2_id, repo_name=self.__class__.__name__)
        self.event_handler.handle(event)

    def update_level2_resources(self, level1_id, records, context=None):
        wrappers = []
        for record in records:
            wrapper = self.update_level2_resource(level1_id, record.id, record, context=context)
            # self.resources[level1_id][wrapper.id] = wrapper
            self.update_resources(level1_id, wrapper.id, wrapper, context=context)
            wrappers.append(wrapper)

        return wrappers

    def get_level1_id(self, instance, context=None):
        lid = instance
        if isinstance(instance, self.wrapper_class):
            lid = instance.id
        return lid

    def get_level1_id_generator(self, context=None):
        ids = self.db.Session.query(self.level_one_model.id).all()
        return list(map(lambda x: x[0], ids))

    def load_all(self, context=None):
        level1_id_generator = self.get_level1_id_generator(context=context)
        for level1_id in level1_id_generator:
            self.load(level1_id, context=context)

    def load(self, level1_id, level2_id=None, context=None):
        lid = self.get_level1_id(level1_id, context=context)

        level1_resources = self.resources.get(lid, None)
        if level1_resources and level2_id is not None:
            level2_resource = level1_resources.get(level2_id, None)
            if level2_resource:
                return [level2_resource]

        records = self.load_level2_resources(lid, level2_id=level2_id, context=context)

        return self.update_level2_resources(lid, records, context=context)

    def get_default_level2_id(self, level1_id, context=None):
        assert False, 'GetWithoutLevelTwoIDError'
        return False, None

    def get_level1_resource(self, level1_id, context=None):
        lid = level1_id
        if isinstance(level1_id, self.level_one_model):
            lid = level1_id.id

        level_one_resource = self.resources.get(lid, None)
        if not level_one_resource:
            self.load(lid, context=context)
            level_one_resource = self.resources.get(lid, None)
            if not level_one_resource:
                import pdb;pdb.set_trace()
                return None

        return lid, level_one_resource

    def list(self, level1_id, weak=True, context=None):
        lid, level_one_resource = self.get_level1_resource(level1_id, context=context)
        ret = []
        for _, v in level_one_resource.items():
            weak or v.ref(context)
            ret.append(v)

        return ret

    def get(self, level1_id, level2_id=None, weak=False, context=None):
        lid, level_one_resource = self.get_level1_resource(level1_id, context=context)
        if level2_id is None:
            ret, level2_id = self.get_default_level2_id(lid, context=context)
            if not ret:
                return

        level2_resource = level_one_resource.get(level2_id, None)
        if not level2_resource:
            self.load(lid, level2_id, context=context)
            level2_resource = level_one_resource.get(level2_id, None)
        # ss and print(f'PRE  Get SS {ss.id} ref={ss.refcnt}')
        context = context if context else self.context
        # context.add_tag('GET')
        level2_resource and level2_resource.ref(context.clone().add_tag('GET'))
        # ss and print(f'POST Get SS {ss.id} ref={ss.refcnt}')
        # level2_resource = ScopedWrapper(level2_resource) if level2_resource else level2_resource
        return level2_resource

    def release(self, resource, context=None):
        context = context if context else self.context
        # context.add_tag('RELEASE')
        resource and resource.unref(context.clone().add_tag('RELEASE'))


class MappingsMixin:
    def update_depend_mappings_resource(self, resource, context=None):
        for mapping_resource_id in resource.mappings:
            self.default_update_depend_resource(attr='mappings', resource=resource,
                    level2_id=mapping_resource_id, context=context)


class ViewMixin:
    view_key = ''
    resource_views = {}

    def add_resource(self, level1_id, level2_id, resource):
        super().add_resource(level1_id, level2_id, resource)
        if not self.view_key: return
        self.resources[level1_id][level2_id] = resource
        l1_views = self.resource_views.get(level1_id, None)
        view_key = getattr(resource, self.view_key)
        if l1_views is None:
            self.resource_views[level1_id] = defaultdict(set)
        self.resource_views[level1_id][view_key].add(resource.id)

    def remove_resource(self, level1_id, level2_id=None):
        resource = super().remove_resource(level1_id, level2_id)
        if resource is None or not self.view_key:
            return resource

        if level2_id is None:
            return self.resource_views.pop(level1_id, None)

        view_key = getattr(resource, self.view_key)

        self.resource_views[level1_id][view_key].remove(level2_id)
        return resource

    # def get_view_ids(self, level1_id, level2_id):
    #     level1_resources = self.resource_views.get(level1_id, None)
    #     if level1_resources is None:
    #         return None
    #     level1_resources.get(level2_id)


class AdvancedLevel2ResourceMgr(ViewMixin, Level2ResourceMgr):
    view_key = ''
    resource_views = {}
    depend_resource_attrs = frozenset()

    def __init__(self, db, depend_resource_mgrs, **kwargs):
        attrs = set(depend_resource_mgrs.keys())
        assert self.depend_resource_attrs == attrs, f'Actual: {attrs}. Expected: {self.depend_resource_attrs}'
        self.depend_resource_mgrs = depend_resource_mgrs
        super().__init__(db=db, **kwargs)

    def get_mapping_resource(self, level1_id, level2_id, context=None):
        mapping_resource = self.mapping_resource_mgr.get(level1_id, level2_id, context=context)
        return mapping_resource

    def default_update_depend_resource(self, attr, resource, level1_id=None, level2_id=None, context=None):
        level1_id = getattr(resource, self.link_key) if level1_id is None else level1_id
        level2_id = getattr(resource, attr) if level2_id is None else level2_id
        mgr = self.depend_resource_mgrs.get(attr)

        depend_resource = mgr.get(level1_id, level2_id, context=context) \
                if isinstance(mgr, Level2ResourceMgr) else mgr.get(level1_id, context=context)

        context = context if context else self.context
        if depend_resource is None:
            logger.error(f'Depend Mgr {mgr} has no ({level1_id}, {level2_id})')
        resource.register_cb(partial(BinUnrefFirstCB, depend_resource, context.clone().add_tag('DUP')))

    def update_level2_resource(self, level1_id, level2_id, record, context=None):
        wrapper = self.wrapper_class(record, cleanup=self.cleanupcb,
                event_handler=self.event_handler, **self.wrapper_kwargs)
        for attr in self.depend_resource_attrs:
            handler = getattr(self, f'update_depend_{attr}_resource', None)
            handler = handler if handler else partial(self.default_update_depend_resource, attr, context=context)
            handler(wrapper)

        return wrapper
