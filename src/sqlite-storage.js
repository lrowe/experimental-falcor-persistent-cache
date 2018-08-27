// @flow
"use strict";

import type { EncodedPath, EncodedValue } from "./encoding";
import type { IStorage, IStorageReader, IStorageReaderWriter } from "./storage";

const { Observable } = require("falcor-observable");
const Database = require("better-sqlite3");

class Connection {
  db: Database;
  checkedOutTo: ?{};
  begin: any;
  rollback: any;
  commit: any;
  set: any;
  getLessThanEqual: any;
  keys: any;
  entries: any;

  constructor(db: Database): void {
    this.db = db;
    this.checkedOutTo = null;
    this.begin = db.prepare("BEGIN");
    this.rollback = db.prepare("ROLLBACK");
    this.commit = db.prepare("COMMIT");
    this.set = db.prepare("INSERT INTO kv (k, v) VALUES (?, ?)");
    this.getLessThanEqual = db.prepare(
      "SELECT k, v from kv WHERE k <= ? ORDER BY k DESC LIMIT 1"
    );
    this.keys = db.prepare("SELECT k from kv ORDER BY k");
    this.entries = db.prepare("SELECT k, v from kv ORDER BY k");
  }
}

class SqliteStorageReader implements IStorageReader {
  connection: Connection;
  constructor(connection: Connection): void {
    this.connection = connection;
  }
  getLessThanEqual(encodedPath: EncodedPath) {
    const row = this.connection.getLessThanEqual.get(encodedPath);
    if (!row) {
      return null;
    }
    const { k, v } = row;
    return [k, v];
  }
}

class SqliteStorageReaderWriter extends SqliteStorageReader
  implements IStorageReaderWriter {
  setPathValue(encodedPath: EncodedPath, encodedValue: EncodedValue): void {
    this.connection.set.run(encodedPath, encodedValue);
  }
}

class SqliteStorage implements IStorage {
  readConnection: Connection;
  writeConnection: Connection;

  constructor(filename: string, options?: {} = {}): void {
    const db = new Database(filename, options);
    db.exec(
      "CREATE TABLE IF NOT EXISTS kv (k BLOB PRIMARY KEY, v BLOB) WITHOUT ROWID"
    );
    // As all reader / writer usage is sync so we only need one of each.
    this.writeConnection = new Connection(db);
    this.readConnection = new Connection(
      new Database(filename, { ...options, readonly: true })
    );
  }

  getReader(): Observable<IStorageReader> {
    return Observable.create(observer => {
      const connection = this.readConnection;
      connection.begin.run();
      observer.onNext(new SqliteStorageReader(connection));
      connection.rollback.run();
      observer.onCompleted();
    });
  }

  getReaderWriter(): Observable<IStorageReaderWriter> {
    return Observable.create(observer => {
      const connection = this.writeConnection;
      connection.begin.run();
      observer.onNext(new SqliteStorageReaderWriter(connection));
      connection.commit.run();
      observer.onCompleted();
    });
  }

  *keys(): Iterable<EncodedPath> {
    const connection = this.readConnection;
    connection.begin.run();
    for (const { k } of connection.keys.iterate()) {
      yield k;
    }
    connection.rollback.run();
  }

  *entries(): Iterable<[EncodedPath, EncodedValue]> {
    const connection = this.readConnection;
    connection.begin.run();
    for (const { k, v } of connection.entries.iterate()) {
      yield [k, v];
    }
    connection.rollback.run();
  }

  close() {
    this.readConnection.db.close();
    this.writeConnection.db.close();
  }
}

module.exports = { SqliteStorage };
