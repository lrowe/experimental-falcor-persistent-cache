// @flow
"use strict";

import type { EncodedPath, EncodedValue } from "./encoding";
import type { IStorage, IStorageReader, IStorageReaderWriter } from "./storage";

const { Observable } = require("falcor-observable");
const lmdb = require("node-lmdb");

class LmdbStorageReader implements IStorageReader {
  txn: any;
  dbi: any;
  cursor: any;
  constructor(txn: any, dbi: any, cursor: any): void {
    this.txn = txn;
    this.dbi = dbi;
    this.cursor = cursor;
  }
  getLessThanEqual(encodedPath: EncodedPath) {
    const { cursor } = this;
    let k = cursor.goToRange(encodedPath);
    if (k !== null && k.compare(encodedPath) !== 0) {
      k = cursor.goToPrev();
    }
    if (k === null) {
      return null;
    }
    const v = cursor.getCurrentBinary();
    return [k, v];
  }
}

class LmdbStorageReaderWriter extends LmdbStorageReader
  implements IStorageReaderWriter {
  setPathValue(encodedPath: EncodedPath, encodedValue: EncodedValue): void {
    this.txn.putBinary(this.dbi, encodedPath, encodedValue);
  }
}

class LmdbStorage implements IStorage {
  env: any;
  dbi: any;

  constructor(envOptions: {} = {}, dbiOptions: {} = {}): void {
    this.env = new lmdb.Env();
    this.env.open(envOptions);
    this.dbi = this.env.openDbi({ ...dbiOptions, keyIsBuffer: true });
  }

  getReader(): Observable<IStorageReader> {
    return Observable.create(observer => {
      const { env, dbi } = this;
      const txn = env.beginTxn({ readOnly: true });
      const cursor = new lmdb.Cursor(txn, dbi);
      observer.onNext(new LmdbStorageReader(txn, dbi, cursor));
      cursor.close();
      txn.abort();
      observer.onCompleted();
    });
  }

  getReaderWriter(): Observable<IStorageReaderWriter> {
    return Observable.create(observer => {
      const { env, dbi } = this;
      const txn = env.beginTxn();
      const cursor = new lmdb.Cursor(txn, dbi);
      observer.onNext(new LmdbStorageReaderWriter(txn, dbi, cursor));
      cursor.close();
      txn.commit();
      observer.onCompleted();
    });
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
