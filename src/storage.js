// @flow
"use strict";
import type { EncodedPath, EncodedValue } from "./encoding";
import type { IObservable } from "falcor-observable";

export interface IStorage {
  getReader(): IObservable<IStorageReader>;
  getReaderWriter(): IObservable<IStorageReaderWriter>;
  close(): void;
}

export interface IStorageReader {
  getLessThanEqual(encodedPath: EncodedPath): ?[EncodedPath, EncodedValue];
}

export interface IStorageWriter {
  setPathValue(encodedPath: EncodedPath, encodedValue: EncodedValue): void;
}

export interface IStorageReaderWriter extends IStorageReader, IStorageWriter {}
