from collections import namedtuple
from dataclasses import dataclass
from datetime import datetime
from itertools import product

import sqlalchemy as sa
from flask import current_app

from db import db
from config import Config


class BaseItem:
    @property
    def json(self):
        return self.__dict__


class ReceivedItem(BaseItem):
    table = sa.Table('InventoryReceived', db.metadata, autoload=True, autoload_with=db.engine)

    def __init__(self, **kwargs):
        for c in self.table.columns:
            setattr(self, c.name, kwargs.get(c.name))

    @classmethod
    def query(cls, item_no, project_site_id, po_no):
        query = sa.select([cls.table])
        query = query.where(cls.table.c.ItemNo == item_no)
        query = query.where(cls.table.c.ProjectSiteId == project_site_id)
        query = query.where(cls.table.c.PONo == po_no)
        query = query.order_by(cls.table.c.ItemNo, cls.table.c.ProjectSiteId, cls.table.c.PONo)
        con = db.engine.connect()
        cols = tuple(c.name for c in cls.table.columns)
        return (cls(**dict(zip(cols, r))) for r in con.execute(query))


class ShippedItem(BaseItem):
    table = sa.Table('InventoryShipped', db.metadata, autoload=True, autoload_with=db.engine)

    def __init__(self, **kwargs):
        for c in self.table.columns:
            setattr(self, c.name, kwargs.get(c.name))

    @classmethod
    def query(cls, item_no, project_site_id, po_no):
        query = sa.select([cls.table])
        query = query.where(cls.table.c.ItemNo == item_no)
        query = query.where(cls.table.c.ProjectSiteId == project_site_id)
        query = query.where(cls.table.c.PONo == po_no)
        query = query.order_by(cls.table.c.ItemNo, cls.table.c.ProjectSiteId, cls.table.c.PONo)
        con = db.engine.connect()
        cols = tuple(c.name for c in cls.table.columns)
        return (cls(**dict(zip(cols, r))) for r in con.execute(query))


class PickedItem(BaseItem):
    table = sa.Table('InventoryPicked', db.metadata, autoload=True, autoload_with=db.engine)

    def __init__(self, **kwargs):
        for c in self.table.columns:
            setattr(self, c.name, kwargs.get(c.name))

    @classmethod
    def query(cls, auth_no, project_site_id):
        query = sa.select([cls.table])
        query = query.where(cls.table.c.AuthNo == auth_no)
        query = query.where(cls.table.c.ProjectSiteId == project_site_id)
        query = query.order_by(cls.table.c.AuthNo, cls.table.c.ProjectSiteId)
        cols = tuple(c.name for c in cls.table.columns)
        con = db.engine.connect()
        return (cls(**dict(zip(cols, r))) for r in con.execute(query))


class HistoryItem(BaseItem):
    table = sa.Table('SiteEquipmentDeploymentHistory', db.metadata, autoload=True, autoload_with=db.engine)

    def __init__(self, **kwargs):
        for c in self.table.columns:
            setattr(self, c.name, kwargs.get(c.name))

    @classmethod
    def query(cls, auth_no, project_site_id):
        query = sa.select([cls.table])
        query = query.where(cls.table.c.AuthNo == auth_no)
        query = query.where(cls.table.c.ProjectSiteId == project_site_id)
        query = query.order_by(cls.table.c.AuthNo, cls.table.c.ProjectSiteId)
        cols = tuple(c.name for c in cls.table.columns)
        con = db.engine.connect()
        return (cls(**dict(zip(cols, r))) for r in con.execute(query))


class LineItem(BaseItem):
    child_items = (ReceivedItem, ShippedItem, PickedItem, HistoryItem)

    def __init__(self, **kwargs):
        for c in {c for item in LineItem.child_items for c in item.table.columns}:
            setattr(self, c.name, kwargs.get(c.name))

    @classmethod
    def query(cls, item_no, site_id, po_no):
        received = list(ReceivedItem.query(item_no, site_id, po_no))
        shipped = list(ShippedItem.query(item_no, site_id, po_no))
        if not received:
            received = [ReceivedItem()]
        if not shipped:
            shipped = [ShippedItem()]
        items = []
        for (r, s) in product(received, shipped):
            item = LineItem(**r.__dict__)
            for (k, v) in s.__dict__.items():
                setattr(item, k, v)
            items.append(item.json)
        return items
