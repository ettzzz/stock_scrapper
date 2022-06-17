#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 15 13:26:05 2022

@author: eee
"""

import pymongo


class BaseMongoOperator:
    def __init__(self, mongo_uri, db_name):
        self.mongo_uri = mongo_uri
        self.db_name = db_name

    def on(self):
        self.cli = pymongo.MongoClient(self.mongo_uri)
        self.db = self.cli[self.db_name]
        return self.db

    def off(self):
        self.cli.close()
        self.db = None

    def fetch_by_command(self, table_name, mongo_query):
        col = self.db[table_name]
        query = col.find(mongo_query)
        return query

    def drop_table(self, table_name, mongo_query={}):
        if self.db is None:
            return
        col = self.db[table_name]
        col.drop(mongo_query)

    def purge_table_with_caution(self, table_name, mongo_query={}):
        if self.db is None:
            return
        col = self.db[table_name]
        col.delete_many(mongo_query)

    def has_table(self, table_name):
        if self.db is None:
            return
        table_list = self.db.list_collection_names()
        return table_name in table_list


if __name__ == "__main__":
    db = BaseMongoOperator(mongo_uri, db_name)
    conn = db.on()
