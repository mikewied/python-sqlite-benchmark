#!/usr/bin/env python

import argparse
import logging
import sqlite3

from cbsnappy import snappy

def connect_db(args):
    # TODO: (1) BFD - connect_db - use pragma max_page_count.

    logging.debug("  connect_db: " + args.db_path)

    db = sqlite3.connect(args.db_path)
    db.text_factory = str
    db.isolation_level = None
    db.execute("PRAGMA page_size=65536")
    db.execute("PRAGMA journal_mode=off")

    return db

def create_db(db, args):
    logging.debug("  create_db: " + args.db_path)

    # The cas column is type text, not integer, because sqlite
    # integer is 63-bits instead of 64-bits.
    db.executescript("""
                BEGIN;
                CREATE TABLE cbb_msg
                    (cmd integer,
                    vbucket_id integer,
                    key blob,
                    flg integer,
                    exp integer,
                    cas text,
                    meta blob,
                    val blob,
                    seqno integer,
                    dtype integer,
                    meta_size integer,
                    conf_res integer);
                COMMIT;
            """)

def load_data(db, args):
    stmt = "INSERT INTO cbb_msg (cmd, vbucket_id, key, flg, exp, cas, meta, val, " + \
        "seqno, dtype, meta_size, conf_res) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

    with open(args.data_path, "r") as lines:
        keys = 0
        values = 0
        batch = 0
        count = 0
        c = db.cursor()
        for line in lines:
            if batch == args.batch_size:
                print count, keys, values, keys+values
                c.execute("commit")
                batch = 0
            if batch == 0:
                c.execute("begin")

            key = "key::" + str(count)
            msg = 87, 0, key, 33554438, 0, 1497912596704526336, 1, line, 1, 1, 0, 0
            cmd, vbucket_id, key, flg, exp, cas, meta, val, seqno, dtype, nmeta, conf_res = msg
            if val != None:
                compressed = snappy.compress(val)
                if len(compressed) < len(val):
                    val = compressed

            keys += len(key)
            values += len(val)

            c.execute(stmt, (cmd, vbucket_id,
                        sqlite3.Binary(key),
                        flg, exp, str(cas),
                        sqlite3.Binary(str(meta)),
                        sqlite3.Binary(val),
                        seqno,
                        dtype,
                        nmeta,
                        conf_res))
            batch = batch + 1
            count = count + 1
    db.commit()

def main():
    parser = argparse.ArgumentParser(description='Process some integers.')
    parser.add_argument('--db-path', dest='db_path', required=True,
                        help='The path to the output sqlite db')
    parser.add_argument('--data-path', dest='data_path', required=True,
                        help='The path to the input data file')
    parser.add_argument('--batch-size', dest='batch_size', default=50000,
                        help='The path to the input data file')
    args = parser.parse_args()

    db = connect_db(args)
    create_db(db, args)
    load_data(db, args)

if __name__ == "__main__":
    main()


