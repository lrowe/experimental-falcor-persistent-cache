// @flow
"use strict";
import type { EncodedPath, EncodedValue } from "./encoding";

export interface IStorage {
  getReader(): IStorageReader;
  getWriter(): IStorageWriter;
  close(): void;
}

export interface IStorageReader {
  getLessThanEqual(encodedPath: EncodedPath): ?[EncodedPath, EncodedValue];
  close(): void;
}

export interface IStorageWriter {
  set(encodedPath: EncodedPath, encodedValue: EncodedValue): void;
  commit(): void;
  abort(): void;
}
