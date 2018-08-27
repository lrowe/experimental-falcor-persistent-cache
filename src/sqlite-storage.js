// @flow
"use strict";

import type { EncodedPath, EncodedValue } from "./encoding";
import type { IStorage, IStorageReader, IStorageWriter } from "./storage";

const Database = require("better-sqlite3");

class Connection {
  db: Database;
  checkedOutTo: ?{};
  _begin: any;
  _rollback: any;
  _commit: any;
  set: any;
  getLessThanEqual: any;
  keys: any;
  entries: any;

  constructor(db: Database): void {
    this.db = db;
    this.checkedOutTo = null;
    this._begin = db.prepare("BEGIN");
    this._rollback = db.prepare("ROLLBACK");
    this._commit = db.prepare("COMMIT");
    this.set = db.prepare("INSERT INTO kv (k, v) VALUES (?, ?)");
    this.getLessThanEqual = db.prepare(
      "SELECT k, v from kv WHERE k <= ? ORDER BY k DESC LIMIT 1"
    );
    this.keys = db.prepare("SELECT k from kv ORDER BY k");
    this.entries = db.prepare("SELECT k, v from kv ORDER BY k");
  }

  checkOut(checkedOutTo: {}): this {
    if (this.checkedOutTo) {
      throw new Error("connection already checked out.");
    }
    this.checkedOutTo = checkedOutTo;
    return this;
  }

  checkIn(checkedOutTo: {}): null {
    if (this.checkedOutTo !== checkedOutTo) {
      throw new Error("connection checked out to someone else.");
    }
    this.checkedOutTo = null;
    if (this.db.inTransaction) {
      this._rollback.run();
    }
    return null;
  }

  begin(): void {
    this._begin.run();
  }

  commit(): void {
    if (this.db.inTransaction) {
      this._commit.run();
    }
  }

  rollback(): void {
    if (this.db.inTransaction) {
      this._rollback.run();
    }
  }

  close(): void {
    this.db.close();
  }
}

class SqliteStorageReader implements IStorageReader {
  connection: ?Connection;

  constructor(connection: Connection): void {
    this.connection = connection.checkOut(this);
    connection.begin();
  }

  getLessThanEqual(encodedPath: EncodedPath): ?[EncodedPath, EncodedValue] {
    const { connection } = this;
    if (!connection) {
      throw new Error("closed");
    }
    const row = connection.getLessThanEqual.get(encodedPath);
    if (!row) {
      return null;
    }
    const { k, v } = row;
    return [k, v];
  }

  close(): void {
    const { connection } = this;
    if (!connection) {
      throw new Error("closed");
    }
    this.connection = connection.checkIn(this);
  }
}

class SqliteStorageWriter implements IStorageWriter {
  connection: ?Connection;

  constructor(connection: Connection): void {
    this.connection = connection.checkOut(this);
    connection.begin();
  }

  set(encodedPath: EncodedPath, encodedValue: EncodedValue): void {
    const { connection } = this;
    if (!connection) {
      throw new Error("closed");
    }
    connection.set.run(encodedPath, encodedValue);
  }

  commit(): void {
    const { connection } = this;
    if (!connection) {
      throw new Error("closed");
    }
    connection.commit();
    this.connection = connection.checkIn(this);
  }

  abort(): void {
    const { connection } = this;
    if (!connection) {
      throw new Error("closed");
    }
    connection.rollback();
    this.connection = connection.checkIn(this);
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

  getReader(): SqliteStorageReader {
    return new SqliteStorageReader(this.readConnection);
  }

  getWriter(): SqliteStorageWriter {
    return new SqliteStorageWriter(this.writeConnection);
  }

  *keys(): Iterable<EncodedPath> {
    const connection = this.readConnection.checkOut(this);
    connection.begin();
    for (const { k } of connection.keys.iterate()) {
      yield k;
    }
    connection.checkIn(this);
  }

  *entries(): Iterable<[EncodedPath, EncodedValue]> {
    const connection = this.readConnection.checkOut(this);
    connection.begin();
    for (const { k, v } of connection.entries.iterate()) {
      yield [k, v];
    }
    connection.checkIn(this);
  }

  close() {
    this.readConnection.close();
    this.writeConnection.close();
  }
}

module.exports = { SqliteStorage };
