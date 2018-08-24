// @flow
"use strict";
import type {
  JsonGraph,
  JsonGraphEnvelope,
  JsonGraphLeaf,
  JsonGraphNode,
  Key,
  KeySet,
  Path,
  PathSet,
  PathValue,
  IDataSource
} from "falcor-json-graph";
export type EncodedKey = string;
export type EncodedPath = string;
export type EncodedPathSet = EncodedKey[][];

const { Observable, tap } = require("falcor-observable");
const { mergeJsonGraphEnvelope, mergeJsonGraph } = require("falcor-json-graph");
const { iterKeySet, iterJsonGraph } = require("./iter");
const lmdb = require("node-lmdb");
const EMPTY_BUFFER = Buffer.alloc(0);

const SEP = " "; // all lower chars are escaped.

function encodeKey(key: Key): EncodedKey {
  return encodeURIComponent(String(key)) + SEP;
}

function decodeKey(encoded: EncodedKey): Key {
  return decodeURIComponent(encoded.slice(0, -1));
}

function encodeKeys(path: Path): EncodedKey[] {
  return path.map(key => encodeKey(key));
}

function encodePath(path: Path): EncodedPath {
  return encodeKeys(path).join("");
}

function splitEncodedPath(encoded: EncodedPath): EncodedKey[] {
  const out = [];
  let sliceStart = 0;
  let sliceEnd;
  while ((sliceEnd = encoded.indexOf(SEP, sliceStart) + 1) !== 0) {
    out.push(encoded.slice(sliceStart, sliceEnd));
    sliceStart = sliceEnd;
  }
  return out;
}

function decodePath(encoded: EncodedPath): Path {
  return encoded
    .slice(0, -1)
    .split(SEP)
    .map(e => decodeURIComponent(e));
}

function encodeKeySet(keySet: KeySet): EncodedKey[] {
  return Array.from(iterKeySet(keySet))
    .map(key => encodeKey(key))
    .sort()
    .reverse();
}

function decodeKeySet(encodedKeySet: EncodedKey[]): KeySet {
  return encodedKeySet.map(encoded => decodeKey(encoded));
}

function encodePathSet(pathSet: PathSet): EncodedPathSet {
  return pathSet.map(keySet => encodeKeySet(keySet));
}

function decodePathSet(encodedPathSet: EncodedPathSet): PathSet {
  return encodedPathSet.map(encodedKeySet => decodeKeySet(encodedKeySet));
}

function rotate(
  indices: number[],
  lengths: number[],
  position: number = indices.length - 1
): number {
  let i = indices.length - 1;
  while (i > position) {
    indices[i] = 0;
    --i;
  }
  while (i >= 0) {
    const next = indices[i] + 1;
    if (next < lengths[i]) {
      indices[i] = next;
      break;
    }
    indices[i] = 0;
    --i;
  }
  return i;
}

function rotateTo(
  indices: number[],
  lengths: number[],
  position: number,
  index: number
): number {
  let i = indices.length - 1;
  while (i > position) {
    indices[i] = 0;
    --i;
  }
  if (index !== -1) {
    indices[i] = index;
    return i;
  }
  indices[i] = 0;
  --i;
  while (i >= 0) {
    const next = indices[i] + 1;
    if (next < lengths[i]) {
      indices[i] = next;
      return i;
    }
    indices[i] = 0;
    --i;
  }
  return i;
}
/*

[["videos"], [2, 1], ["foo", "bar"]]
"videos,2,foo," -> ["videos,2,", null]
"videos,2,bar,"
"videos,1,foo,"
"videos,1,bar,"

"videos,2,foo," -> ["videos,1,zzz,", null]
"videos,2,bar," -> missing
"videos,1,foo,"
"videos,1,bar,"

"videos,2,foo," -> ["videos,1,aaa,", null]
fdi = 1
commonPrefix = [["videos"]]
suffix = [["foo", "bar"]]
"videos,2,bar," -> missing
"videos,1,foo," -> missing
"videos,1,bar," -> missing

"videos,2,foo," -> ["videos,1,", null]
"videos,2,bar," -> missing
"videos,1,foo," -> missing
"videos,1,bar," -> missing

*/

function firstDifferentIndex(
  foundKeys: EncodedKey[],
  encodedKeys: EncodedKey[]
): number {
  return foundKeys.findIndex((k, i) => encodedKeys[i] !== k);
}

function encodedKeysFromIndices(
  indices: number[],
  encodedPathSet: EncodedPathSet
): EncodedKey[] {
  return indices.map((j, i) => encodedPathSet[i][j]);
}

/*
Calculate the sections of pathset between two paths.
[[5, 4, 3, 2, 1, 0], [5, 4, 3, 2, 1, 0], [5, 4, 3, 2, 1, 0], [5, 4, 3, 2, 1, 0]]
[3, 3, 3, 3]
[3, 1, 1]
[
 [ [3], [3],       [3]         ,       [3, 2, 1, 0] ],
 [ [3], [3],          [2, 1, 0], [5, 4, 3, 2, 1, 0] ],
 [ [3], [2], [5, 4, 3, 2, 1, 0], [5, 4, 3, 2, 1, 0] ],
 [ [3], [1], [5, 4, 3, 2]      , [5, 4, 3, 2, 1, 0] ],
]
*/
function betweenPaths(
  encodedPathSet: EncodedPathSet,
  upperIndices: number[],
  lengths: number[],
  upperKeys: EncodedKey[],
  lowerKeys: EncodedKey[],
  fdi: number,
  missing: EncodedPathSet[]
): void {
  // upper
  for (
    let i = fdi,
      length = encodedPathSet.length,
      upperKeySets = upperKeys.map(k => [k]);
    i < length;
    ++i
  ) {
    const cut = upperIndices[i];
    const keySetLength = lengths[i];
    if (cut !== keySetLength - 1) {
      const left = upperKeySets.slice(0, i);
      const middle = encodedPathSet[i].slice(cut, keySetLength);
      const right = encodedPathSet.slice(i + 1);
      missing.push(left.concat([middle], right));
    }
  }

  // lower
  for (
    let i = fdi + 1,
      length = Math.min(lowerKeys.length, encodedPathSet.length),
      lowerKeySets = lowerKeys.slice(0, length).map(k => [k]);
    i < length;
    ++i
  ) {
    const key = lowerKeys[i];
    const cut = encodedPathSet[i].findIndex(k => k <= key);
    if (cut !== 0) {
      const left = lowerKeySets.slice(0, i);
      const keySet = encodedPathSet[i];
      const middle = keySet.slice(0, cut === -1 ? keySet.length : cut);
      const right = encodedPathSet.slice(i + 1);
      missing.push(left.concat([middle], right));
    }
  }
}

function walkCache(
  pathSets: PathSet[],
  getClosestEntry: (encodedPath: EncodedPath) => [EncodedPath, JsonGraphLeaf]
): {
  found: { [encodedPath: EncodedPath]: JsonGraphLeaf },
  missing: EncodedPathSet[]
} {
  const encodedPathSets = pathSets.map(pathSet => encodePathSet(pathSet));
  const found = {};
  const missing = [];
  const remaining = [...encodedPathSets];
  while (remaining.length > 0) {
    // should probably optimize the paths here.
    const encodedPathSet = remaining.shift();
    _walkCache(encodedPathSet, remaining, found, missing, getClosestEntry);
  }
  return { found, missing };
}

function _walkCache(
  encodedPathSet: EncodedPathSet,
  remaining: EncodedPathSet[],
  found: { [encodedPath: EncodedPath]: JsonGraphLeaf },
  missing: EncodedPathSet[],
  getClosestEntry: (encodedPath: EncodedPath) => [EncodedPath, JsonGraphLeaf]
): void {
  const lengths = encodedPathSet.map(component => component.length);
  if (lengths.some(length => length === 0)) {
    return;
  }

  const indices = Array(lengths.length).fill(0);
  let incremented = indices.length - 1;
  while (incremented >= 0) {
    const encodedKeys = encodedKeysFromIndices(indices, encodedPathSet);
    const encodedPath = encodedKeys.join("");

    if (typeof found[encodedPath] !== "undefined") {
      incremented = rotate(indices, lengths);
      continue;
    }

    const [foundPath, value] = getClosestEntry(encodedPath);

    if (foundPath === encodedPath) {
      found[foundPath] = value;
      incremented = rotate(indices, lengths);
      continue;
    }

    const foundKeys = splitEncodedPath(foundPath);
    const fdi = firstDifferentIndex(foundKeys, encodedKeys);

    // equivalent to encodedPath.startsWith(foundPath)
    if (fdi === -1) {
      if (indices.slice(foundKeys.length).some(j => j !== 0)) {
        throw new Error(
          `Cache is malformed - cache entry found for ${foundPath} which has descendants`
        );
      }
      found[foundPath] = value;
      if (
        typeof value === "object" &&
        value !== null &&
        value.$type === "ref"
      ) {
        // follow reference
        const refPathSet = encodePathSet(value.value);
        const remainingPathSet = encodedPathSet.slice(foundKeys.length);
        const optimizedPath = [...refPathSet, ...remainingPathSet];
        remaining.push(optimizedPath);
      }
      incremented = rotate(indices, lengths, foundKeys.length - 1);
      continue;
    }

    betweenPaths(
      encodedPathSet,
      indices,
      lengths,
      encodedKeys,
      foundKeys,
      fdi,
      missing
    );
    const fdk = foundKeys[fdi];
    const toIndex = encodedPathSet[fdi].findIndex(k => k < fdk);
    incremented = rotateTo(indices, lengths, fdi, toIndex);
    continue;

    /*
    while (encodedPath > foundPath) {
      missing.push(encodedPath);
      incremented = rotate(indices, lengths);
      if (incremented < 0) {
        break;
      }
      encodedKeys = encodedKeysFromIndices(indices, encodedPathSet);
      encodedPath = encodedKeys.join("");
    }
    continue;
    */
  }
}

function jsonGraphFromPathValues(
  pvs: Iterable<PathValue>,
  seed: JsonGraph = {}
): JsonGraph {
  let jsonGraph = seed;
  for (const { path, value } of pvs) {
    const pvjg: any = path.reduceRight((acc, cur) => {
      const branch = {};
      for (const key of iterKeySet(cur)) {
        branch[String(key)] = acc;
      }
      return branch;
    }, value);
    jsonGraph = mergeJsonGraph(jsonGraph, pvjg);
  }
  return jsonGraph;
}

class CacheDataSource implements IDataSource {
  source: ?IDataSource;
  env: any;
  dbi: any;

  constructor(
    source: ?IDataSource,
    envOptions: {} = {},
    dbiOptions: {} = {}
  ): void {
    this.source = source;
    this.env = new lmdb.Env();
    this.env.open(envOptions);
    this.dbi = this.env.openDbi({ ...dbiOptions, keyIsBuffer: true });
  }

  get(pathSets: PathSet[]): Observable<JsonGraphEnvelope> {
    return Observable.create(observer => {
      const txn = this.env.beginTxn({ readOnly: true });
      const cursor = new lmdb.Cursor(txn, this.dbi);
      const { found, missing } = walkCache(
        pathSets,
        this._getClosestEntry.bind(this, cursor)
      );
      cursor.close();
      txn.abort();
      // $FlowFixMe: Why is value mixed here?
      const pbvs: PathValue[] = Object.entries(found).map(
        ([encoded, value]) => ({
          path: decodePath(encoded),
          value
        })
      );
      const jsonGraph = jsonGraphFromPathValues(pbvs);
      let envelope = { jsonGraph };
      if (missing.length === 0 || !this.source) {
        observer.onNext(envelope);
        observer.onCompleted();
        return;
      }
      this.source.get(missing.map(encoded => decodePathSet(encoded))).subscribe(
        remoteResult => {
          this._setCache(remoteResult);
          envelope = mergeJsonGraphEnvelope(envelope, remoteResult);
        },
        err => observer.onError(err),
        () => {
          observer.onNext(envelope);
          observer.onCompleted();
        }
      );
    });
  }

  set(jsonGraphEnvelope: JsonGraphEnvelope): Observable<JsonGraphEnvelope> {
    if (!this.source) {
      throw new Error("must have a source for set");
    }
    return Observable.from(this.source.set(jsonGraphEnvelope)).pipe(
      tap(remoteValue => {
        this._setCache(remoteValue);
      })
    );
  }

  _getClosestEntry(
    cursor: any,
    encodedPath: EncodedPath
  ): [EncodedPath, JsonGraphLeaf] {
    let k = (
      cursor.goToRange(Buffer.from(encodedPath)) || EMPTY_BUFFER
    ).toString();
    if (k !== encodedPath) {
      k = (cursor.goToPrev() || EMPTY_BUFFER).toString();
    }
    if (k === "") {
      return ["", null];
    }
    const v = cursor.getCurrentBinary();
    return [k, JSON.parse(v.toString())];
  }

  _setCache({ jsonGraph }: JsonGraphEnvelope): void {
    const txn = this.env.beginTxn();
    for (const { path, value } of iterJsonGraph(jsonGraph)) {
      const encodedPath = encodePath(path);
      const encodedValue = JSON.stringify(value);
      txn.putBinary(
        this.dbi,
        Buffer.from(encodedPath),
        Buffer.from(encodedValue)
      );
    }
    txn.commit();
  }

  call(
    callPath: Path,
    args?: JsonGraphNode[] = [],
    refPaths?: PathSet[] = [],
    thisPaths?: PathSet[] = []
  ): Observable<JsonGraphEnvelope> {
    if (!this.source) {
      throw new Error("must have a source for call");
    }
    return Observable.from(
      this.source.call(callPath, args, refPaths, thisPaths)
    ).pipe(
      tap(remoteValue => {
        this._setCache(remoteValue);
      })
    );
  }

  *keys(): Iterable<EncodedPath> {
    const txn = this.env.beginTxn({ readOnly: true });
    const cursor = new lmdb.Cursor(txn, this.dbi);
    let found = cursor.goToFirst();
    while (found !== null) {
      yield found.toString();
      found = cursor.goToNext();
    }
    cursor.close();
    txn.commit();
  }

  close() {
    this.dbi.close();
    this.env.close();
  }
}

module.exports = { encodePath, decodePath, CacheDataSource };
