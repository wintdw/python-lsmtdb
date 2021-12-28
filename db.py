#!/usr/bin/env python3

import json
import logging
from pathlib import Path

DBPATH = "/var/lib/lsm/"
logging.basicConfig(level=logging.DEBUG)

class LSMTree():
    def __init__(self, dbpath=DBPATH):
        self.memtable = {}

        # Paths
        self.path = dbpath
        self.journal_file = self.path + "journal"
        self.wal_file = self.path + "wal"
        self.db_file = self.path + "db"
        Path(self.path).mkdir(exist_ok=True)
        Path(self.journal_file).touch(exist_ok=True)
        Path(self.wal_file).touch(exist_ok=True)
        Path(self.db_file).touch(exist_ok=True)

        # Tombstone
        self.tombstone = "delete_tombstone"

        # Configs 
        self.memtable_obj_limit = 5
        self.wal_obj_limit = 5
        self.journal_line_limit = 5

    def upsert(self, key, value):
        # journaling for durable writes
        log = self.journal(key, value)
        # update value in memtable
        self.memtable[key] = value

        # flush if keys >= memtable_objs
        logging.debug("self.memtable: {}".format(self.memtable))
        logging.debug("memtable_objs: {}".format(len(self.memtable)))
        if len(self.memtable) >= self.memtable_obj_limit:
            self.flush()

    def delete(self, key):
        self.upsert(key, self.tombstone)

    def get(self, key):
        data = None
        # check if value is in memtable
        if key in self.memtable:
            data = self.memtable[key]
        else:
            # else check all sstables (wal + db)
            wal = self.load(self.wal_file)
            if key in wal:
                data = wal[key]
            else:
                db = self.load(self.db_file)
                if key in db:
                    data = db[key]

        # If tombstone 
        if data == self.tombstone:
            data = None
    
        return data

    def journal(self, key, value):
        data = {key: value}
        json.dump(data, open(self.journal_file, "a"))
        with open(self.journal_file, "a") as f:
            f.write("\n")

        self.rotate()

    # rotate journal
    def rotate(self):
        data = []
        with open(self.journal_file, "r") as f:
            data = f.read().splitlines()

        if len(data) >= self.journal_line_limit:
            logging.debug("event: ROTATING")
            new_data = data[-1]
            with open(self.journal_file, "w") as f:
                f.write(new_data)
                f.write("\n")

    # flush memtable to wal
    def flush(self):
        wal = self.load(self.wal_file)
        merged = {**wal, **self.memtable}

        logging.debug("wal: {}".format(wal))
        logging.debug("wal_objs: {}".format(len(wal)))
        logging.debug("event: FLUSHING")

        json.dump(merged, open(self.wal_file, "w"))
        # clear memtable 
        self.memtable = {}

        # if objs >= wal_obj_limit, merge with main db for persistence
        if len(wal) >= self.wal_obj_limit:
            self.merge()

    # For merging main db + wal
    def merge(self):
        wal = self.load(self.wal_file)
        db = self.load(self.db_file)
        # Dict merge python 3.5+ semantics
        merged = {**db, **wal}
        # remove tombstone
        removed = []
        for k in merged:
            if merged[k] == self.tombstone:
                removed.append(k)
        for k in removed:
            merged.pop(k)

        logging.debug("event: MERGING")

        json.dump(merged, open(self.db_file, "w"))

        # clear wal
        with open(self.wal_file, "w") as f:
            f.truncate()
            
    def load(self, db_file):
        try:
            db = json.load(open(db_file, "r"))
        except:
            db = {}
        return db

    # for durability
    def shutdown(self):
        logging.debug("event: SHUTTING DOWN")
        self.flush()
        self.merge()
        self.rotate()

    # for integrity
    def startup(self):
        logging.debug("event: STARTING")
        data = {}
        with open(self.journal_file, "r") as f:
            for line in f.readlines():
                jline = json.loads(line)
                # merge
                data = {**data, **jline}
        self.memtable = data

l = LSMTree()
l.startup()
l.upsert("1", "1")
l.upsert("2", "2")
l.upsert("2", "3")
l.upsert("3", "3")
l.upsert("4", "4")
l.upsert("5", "5")
l.upsert("6", "6")
l.delete("7")
print(l.get("7"))
l.shutdown()
