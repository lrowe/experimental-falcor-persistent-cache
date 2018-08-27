// @flow
"use strict";

import type { EncodedPath, EncodedValue } from "./encoding";
import type { IStorage, IStorageReader, IStorageWriter } from "./storage";

const Database = require("better-sqlite3");

class ReadConnection implements IStorageReader {
  db: any;
  _begin: any;
  _rollback: any;
  _getLessThanEqual: any;
  _keys: any;
  _entries: any;

  constructor(filename: string, options: {}): void {
    this.db = new Database(filename, { ...options, readonly: true });
    this._begin = this.db.prepare("BEGIN");
    this._rollback = this.db.prepare("ROLLBACK");
    this._getLessThanEqual = this.db.prepare(
      "SELECT k, v from kv WHERE k <= ? ORDER BY k DESC LIMIT 1"
    );
    this._keys = this.db.prepare("SELECT k from kv ORDER BY k");
    this._entries = this.db.prepare("SELECT k, v from kv ORDER BY k");
    this._begin.run();
  }

  getLessThanEqual(encodedPath: EncodedPath): ?[EncodedPath, EncodedValue] {
    const row = this._getLessThanEqual.get(encodedPath);
    if (!row) {
      return null;
    }
    const { k, v } = row;
    return [k, v];
  }

  *keys(): Iterable<EncodedPath> {
    for (const { k } of this._keys.iterate()) {
      yield k;
    }
  }

  *entries(): Iterable<[EncodedPath, EncodedValue]> {
    for (const { k, v } of this._entries.iterate()) {
      yield [k, v];
    }
  }

  close(): void {
    if (this.db.inTransaction) {
      this._rollback.run();
    }
    this.db.close();
  }
}

class WriteConnection implements IStorageWriter {
  db: any;
  _begin: any;
  _rollback: any;
  _commit: any;
  _set: any;

  constructor(filename: string, options: {}): void {
    this.db = new Database(filename, options);
    this._begin = this.db.prepare("BEGIN");
    this._rollback = this.db.prepare("ROLLBACK");
    this._commit = this.db.prepare("COMMIT");
    this._set = this.db.prepare("INSERT INTO kv (k, v) VALUES (?, ?)");
    this._begin.run();
  }

  set(encodedPath, encodedValue) {
    this._set.run(encodedPath, encodedValue);
  }

  commit() {
    if (this.db.inTransaction) {
      this._commit.run();
    }
    this.db.close();
  }

  abort() {
    if (this.db.inTransaction) {
      this._rollback.run();
    }
    this.db.close();
  }
}

class SqliteStorage implements IStorage {
  filename: string;
  options: {};

  constructor(filename: string, options?: {} = {}): void {
    this.filename = filename;
    this.options = options;
    const db = new Database(filename, options);
    db.exec(
      "CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID"
    );
    db.close();
  }

  getReader(): ReadConnection {
    return new ReadConnection(this.filename, this.options);
  }

  getWriter(): WriteConnection {
    return new WriteConnection(this.filename, this.options);
  }

  *keys(): Iterable<EncodedPath> {
    const reader = this.getReader();
    yield* reader.keys();
    reader.close();
  }

  *entries(): Iterable<[EncodedPath, EncodedValue]> {
    const reader = this.getReader();
    yield* reader.entries();
    reader.close();
  }

  close() {}
}

module.exports = { SqliteStorage };
