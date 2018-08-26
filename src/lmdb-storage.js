// @flow
"use strict";

import type { EncodedPath, EncodedValue } from "./encoding";
import type { IStorage, IStorageReader, IStorageWriter } from "./storage";

const lmdb = require("node-lmdb");

class LmdbStorage implements IStorage {
  env: any;
  dbi: any;

  constructor(envOptions: {} = {}, dbiOptions: {} = {}): void {
    this.env = new lmdb.Env();
    this.env.open(envOptions);
    this.dbi = this.env.openDbi({ ...dbiOptions, keyIsBuffer: true });
  }

  getReader(): IStorageReader {
    const txn = this.env.beginTxn({ readOnly: true });
    const cursor = new lmdb.Cursor(txn, this.dbi);
    return {
      txn,
      cursor,
      getLessThanEqual(encodedPath) {
        let k = this.cursor.goToRange(encodedPath);
        if (k !== null && k.compare(encodedPath) !== 0) {
          k = this.cursor.goToPrev();
        }
        if (k === null) {
          return null;
        }
        const v = this.cursor.getCurrentBinary();
        return [k, v];
      },
      close() {
        this.cursor.close();
        this.txn.abort();
      }
    };
  }

  getWriter(): IStorageWriter {
    const txn = this.env.beginTxn();
    const dbi = this.dbi;
    return {
      dbi,
      txn,
      set(encodedPath, encodedValue) {
        this.txn.putBinary(this.dbi, encodedPath, encodedValue);
      },
      commit() {
        this.txn.commit();
      },
      abort() {
        this.txn.abort();
      }
    };
  }

  *keys(): Iterable<EncodedPath> {
    const txn = this.env.beginTxn({ readOnly: true });
    const cursor = new lmdb.Cursor(txn, this.dbi);
    let found = cursor.goToFirst();
    while (found !== null) {
      yield found;
      found = cursor.goToNext();
    }
    cursor.close();
    txn.abort();
  }

  *entries(): Iterable<[EncodedPath, EncodedValue]> {
    const txn = this.env.beginTxn({ readOnly: true });
    const cursor = new lmdb.Cursor(txn, this.dbi);
    let k = cursor.goToFirst();
    while (k !== null) {
      const v = cursor.getCurrentBinary();
      yield [k, v];
      k = cursor.goToNext();
    }
    cursor.close();
    txn.abort();
  }

  close() {
    this.dbi.close();
    this.env.close();
  }
}

module.exports = { LmdbStorage };
