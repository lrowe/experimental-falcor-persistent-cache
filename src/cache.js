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
import type { PathTree, LengthTree } from "falcor-path-utils";
export type EncodedKey = string;
export type EncodedPath = Buffer;
export type EncodedLeaf = Buffer;
export type EncodedPathTreeEntry = [EncodedKey, EncodedPathTree | null];
export type EncodedPathTree = EncodedPathTreeEntry[];

export type JsonGraphEnvelopeWithMissingPaths = {
  jsonGraph: JsonGraph,
  paths?: PathSet[],
  missingPaths?: PathSet[],
  invalidated?: PathSet[],
  context?: JsonGraph
};

const { Observable, tap } = require("falcor-observable");
const { mergeJsonGraph } = require("falcor-json-graph");
const { toPaths } = require("falcor-path-utils");
const { iterKeySet, iterJsonGraph } = require("./iter");
const lmdb = require("node-lmdb");

const SEP = " "; // all lower chars are escaped.
const SEP_BUFFER = Buffer.from(SEP);

function encodeKey(key: Key): EncodedKey {
  return encodeURIComponent(String(key));
}

function decodeKey(encoded: EncodedKey): Key {
  return decodeURIComponent(encoded);
}

function encodeKeys(path: Path): EncodedKey[] {
  return path.map(key => encodeKey(key));
}

function decodeKeys(encodedKeys: EncodedKey[]): Path {
  return encodedKeys.map(encoded => decodeKey(encoded));
}

function joinEncodedKeys(encodedKeys: EncodedKey[]): EncodedPath {
  return Buffer.from(encodedKeys.join(SEP) + SEP);
}

function splitEncodedPath(encoded: EncodedPath): EncodedKey[] {
  const out = [];
  let sliceStart = 0;
  let sliceEnd;
  while ((sliceEnd = encoded.indexOf(SEP_BUFFER, sliceStart)) !== -1) {
    out.push(encoded.slice(sliceStart, sliceEnd).toString());
    sliceStart = sliceEnd + 1;
  }
  return out;
}

function encodePath(path: Path): EncodedPath {
  return joinEncodedKeys(encodeKeys(path));
}

function decodePath(encoded: EncodedPath): Path {
  return decodeKeys(splitEncodedPath(encoded));
}

function encodeLeaf(value: JsonGraphLeaf): EncodedLeaf {
  return Buffer.from(JSON.stringify(value));
}

function decodeLeaf(encoded: EncodedLeaf): JsonGraphLeaf {
  return JSON.parse(encoded.toString());
}

/* Return EncodedPathTree for pathSets.
 * Supply subtree when optimizing an existing EncodedPathTree.
 */
function encodePathTree(
  pathSets: PathSet[],
  subtree: EncodedPathTree | null = null
): EncodedPathTree | null {
  return pathSets
    .map(pathSet =>
      pathSet.reduceRight(
        (right, keySet) =>
          Array.from(iterKeySet(keySet))
            .map(key => encodeKey(key))
            .sort()
            .reverse()
            .map(encodedKey => [encodedKey, right]),
        subtree
      )
    )
    .reduce((acc, cur) => mergeEncodedPathTree(acc, cur), null);
}

/* Immutably merge EncodedPathTrees maintaining sorted order (highest first).
 */
function mergeEncodedPathTree(
  left: EncodedPathTree | null,
  right: EncodedPathTree | null
): EncodedPathTree | null {
  if (right === null) {
    return left;
  }
  if (left === null) {
    return right;
  }
  if (left === right) {
    return left;
  }
  const merged = [];
  let leftIndex = 0;
  let rightIndex = 0;
  let leftEntry = left[leftIndex];
  let rightEntry = right[rightIndex];
  while (leftEntry !== undefined || rightEntry !== undefined) {
    while (
      leftEntry !== undefined &&
      (rightEntry === undefined || leftEntry[0] > rightEntry[0])
    ) {
      merged.push(leftEntry);
      leftIndex++;
      leftEntry = left[leftIndex];
    }
    while (
      rightEntry !== undefined &&
      (leftEntry === undefined || rightEntry[0] > leftEntry[0])
    ) {
      merged.push(rightEntry);
      rightIndex++;
      rightEntry = right[rightIndex];
    }
    while (
      leftEntry !== undefined &&
      rightEntry !== undefined &&
      leftEntry[0] === rightEntry[0]
    ) {
      const key = leftEntry[0];
      const value = mergeEncodedPathTree(leftEntry[1], rightEntry[1]);
      merged.push([key, value]);
      leftIndex++;
      leftEntry = left[leftIndex];
      rightIndex++;
      rightEntry = right[rightIndex];
    }
  }
  return merged;
}

function encodedPathTreeToPathSets(
  encodedPathTree: EncodedPathTree
): PathSet[] {
  return toPaths(encodedPathTreeToLengthTree(encodedPathTree));
}

function encodedPathTreeToLengthTree(
  encodedPathTree: EncodedPathTree
): LengthTree {
  const lengthTree = {};
  _encodedPathTreeToLengthTree(lengthTree, encodedPathTree, []);
  return lengthTree;
}

function _lengthTreeParentNode(
  lengthTree: LengthTree,
  prefix: string[]
): PathTree {
  const length = prefix.length + 1;
  let node = lengthTree[length];
  if (node === undefined) {
    node = lengthTree[length] = {};
  }
  for (const key of prefix) {
    let child = node[key];
    if (node === null) {
      throw new Error("Should be unreachable");
    }
    if (child === undefined) {
      child = node[key] = {};
    }
    node = child;
  }
  return node;
}

function _encodedPathTreeToLengthTree(
  lengthTree: LengthTree,
  encodedPathTree: EncodedPathTree,
  prefix: string[]
): void {
  let lengthTreeNode;
  for (const [encodedKey, child] of encodedPathTree) {
    const key = String(decodeKey(encodedKey));
    if (child === null) {
      if (lengthTreeNode === undefined) {
        lengthTreeNode = _lengthTreeParentNode(lengthTree, prefix);
      }
      lengthTreeNode[key] = null;
    } else {
      _encodedPathTreeToLengthTree(lengthTree, child, [...prefix, key]);
    }
  }
}

// tree, index, parent
export type TreeCursor = [EncodedPathTree, number, TreeCursor | null];

function treeCursor(root: EncodedPathTree): TreeCursor | null {
  const cursor = [[["", root]], 0, null];
  return firstTreeCursor(cursor);
}

/* Get a cursor to the first leaf node child of current (maybe current)
*/
function firstTreeCursor(current: TreeCursor): TreeCursor | null {
  let [tree, index, parent] = current;
  if (index >= tree.length) {
    if (parent === null) {
      return null;
    }
    const [ptree, pindex, pparent] = parent;
    return firstTreeCursor([ptree, pindex + 1, pparent]);
  }
  const [, subtree] = tree[index];
  if (subtree === null) {
    return current;
  }
  let found = firstTreeCursor([subtree, 0, current]);
  if (found !== null) {
    return found;
  }
  // This should only happen if subtree is an empty array or it has no
  // non-branch descendants.
  return firstTreeCursor([tree, index + 1, parent]);
}

/* Get a cursor to the first leaf node after current
 */
function nextTreeCursor(current: TreeCursor): TreeCursor | null {
  const [tree, index, parent] = current;
  return firstTreeCursor([tree, index + 1, parent]);
}

/*
current = cursor(tree(["foo"]), index("bar"), cursor(tree([]), index("foo"), null))
searchedKeys = ["foo", "bar"];


foundKeys = ["foo", "baz"];
fdi = 1;
ancestor = tree(["foo"])

foundKeys = ["qux"];
fdi = 0;
ancestor = tree([])


*/

/* Return the closest ancestor cursor of current along the path foundKeys.
 */
function closestCommonAncestorTreeCursor(
  current: TreeCursor,
  currentKeys: EncodedKey[],
  foundKeys: EncodedKey[]
): {
  ancestor: TreeCursor,
  remainingPath: ?(EncodedKey[])
} {
  const fdi = firstDifferentIndex(foundKeys, currentKeys);
  let cursor = current;
  let i =
    fdi === -1
      ? currentKeys.length - foundKeys.length
      : currentKeys.length - fdi;
  while (i > 0) {
    const [, , parent] = cursor;
    if (parent === null) {
      throw new Error("unreachable");
    }
    cursor = parent;
    --i;
  }
  const ancestor = cursor;
  const remainingPath = fdi === -1 ? null : foundKeys.slice(fdi);
  return { ancestor, remainingPath };
}

/* Return the first leaf cursor beyond the relative path.
 */
function traverseTreeCursor(
  current: TreeCursor,
  path: EncodedKey[]
): TreeCursor | null {
  if (path.length === 0) {
    return firstTreeCursor(current);
  }
  const [tree, index, parent] = current;
  const [key, ...restPath] = path;
  let i = index;
  while (i < tree.length) {
    const [k, subtree] = tree[i];
    if (k > key) {
      ++i;
      continue;
    }
    const child = [tree, i, parent];
    if (subtree === null) {
      return child;
    }
    if (k !== key) {
      return firstTreeCursor(child);
    }
    return traverseTreeCursor(child, restPath);
  }
  return firstTreeCursor([tree, i, parent]);
}

/* Return the EncodedPathTree without the section above indicesPath
 * (including the indicesPath bound itself.)
 */
function belowUpperBoundInclusive(
  branch: EncodedPathTree,
  indicesPath: number[]
): EncodedPathTree {
  if (indicesPath.length === 0) {
    throw new Error("unreachable");
  }
  const [index, ...remaining] = indicesPath;
  const [key, child] = branch[index];
  if (child === null) {
    return branch.slice(index);
  }
  const newChild = belowUpperBoundInclusive(child, remaining);
  return [[key, newChild], ...branch.slice(index + 1)];
}

/* Return the EncodedPathTree without the section below indicesPath
 * (excluding the indicesPath bound itself.)
 */
function aboveLowerBoundExclusive(
  branch: EncodedPathTree,
  indicesPath: number[]
): EncodedPathTree | null {
  if (indicesPath.length === 0) {
    throw new Error("unreachable");
  }
  const [index, ...remaining] = indicesPath;
  const [key, child] = branch[index];
  if (child === null) {
    return branch.slice(0, index);
  }
  const newChild = aboveLowerBoundExclusive(child, remaining);
  return [...branch.slice(0, index), [key, newChild]];
}

/* Return the EncodedPathTree between the current cursor (inclusive) and the
 * next cursor (exclusive).
 */
function betweenTreeCursors(
  ancestor: TreeCursor,
  current: TreeCursor,
  next: TreeCursor | null
): EncodedPathTree | null {
  const [tree, index] = ancestor;
  let [, commonRoot] = tree[index];
  if (commonRoot === null) {
    return null;
  }
  if (next !== null) {
    const lowerIndices = indicesFromTreeCursor(next, ancestor);
    commonRoot = aboveLowerBoundExclusive(commonRoot, lowerIndices);
  }
  if (commonRoot === null) {
    return null;
  }
  const upperIndices = indicesFromTreeCursor(current, ancestor);
  return belowUpperBoundInclusive(commonRoot, upperIndices);
}

/* Return the encoded key path between ancestor and cursor.
 */
function encodedKeysFromTreeCursor(
  cursor: TreeCursor,
  ancestor: TreeCursor | null = null
): EncodedKey[] {
  const [tree, index, parent] = cursor;
  if (parent === ancestor || parent === null) {
    // root cursor
    return [];
  }
  const [key] = tree[index];
  return [...encodedKeysFromTreeCursor(parent, ancestor), key];
}

/* Return the indices path between ancestor and cursor.
 */
function indicesFromTreeCursor(
  cursor: TreeCursor,
  ancestor: TreeCursor | null = null
): number[] {
  const [, index, parent] = cursor;
  if (parent === ancestor || parent === null) {
    // root cursor
    return [];
  }
  return [...indicesFromTreeCursor(parent, ancestor), index];
}

function firstDifferentIndex(
  foundKeys: EncodedKey[],
  searchedKeys: EncodedKey[]
): number {
  return foundKeys.findIndex((k, i) => searchedKeys[i] !== k);
}

function isRef(node: JsonGraphNode): boolean %checks {
  return typeof node === "object" && node !== null && node.$type === "ref";
}

function isBranch(node: JsonGraphNode): boolean %checks {
  return typeof node === "object" && node !== null && node.$type === undefined;
}

function walkCache(
  encodedPathTree: EncodedPathTree,
  reader: StorageReader,
  initialJsonGraph: JsonGraph
): JsonGraphEnvelopeWithMissingPaths {
  let jsonGraph = initialJsonGraph;
  let missing = null;
  let remaining = encodedPathTree;
  while (remaining !== null) {
    ({ remaining, missing, jsonGraph } = _walkCache(
      remaining,
      jsonGraph,
      missing,
      reader
    ));
  }
  if (missing) {
    const missingPaths = encodedPathTreeToPathSets(missing);
    return { jsonGraph, missingPaths };
  }
  // XXX could probably set paths here too.
  return { jsonGraph };
}

function _walkCache(
  encodedPathTree: EncodedPathTree,
  initialJsonGraph: JsonGraph,
  missingIn: EncodedPathTree | null,
  reader: StorageReader
): {
  jsonGraph: JsonGraph,
  remaining: EncodedPathTree | null,
  missing: EncodedPathTree | null
} {
  let jsonGraph = initialJsonGraph;
  let remaining = null;
  let missing = missingIn;
  const rootCursor = treeCursor(encodedPathTree);
  if (rootCursor === null) {
    return { jsonGraph, remaining, missing };
  }
  let cursor = rootCursor;
  while (cursor !== null) {
    const encodedKeys = encodedKeysFromTreeCursor(cursor);

    // First look in our already found jsonGraph
    const cursorPath = decodeKeys(encodedKeys);
    const foundPV = traverseJsonGraphOnce(jsonGraph, cursorPath);
    if (foundPV) {
      const { path, value } = foundPV;

      if (cursorPath.length === path.length) {
        cursor = nextTreeCursor(cursor);
        continue;
      }

      const foundKeys = encodeKeys(path);
      const { ancestor } = closestCommonAncestorTreeCursor(
        cursor,
        encodedKeys,
        foundKeys
      );

      if (isRef(value)) {
        // optimize path tree below ancestor onto the ref target.
        const [tree, index] = ancestor;
        const [, subtree] = tree[index];
        const optimized = encodePathTree([(value.value: any)], subtree);
        remaining = mergeEncodedPathTree(remaining, optimized);
      }

      // If value is not a ref we can simply skip over the ancestor's subtree.
      cursor = nextTreeCursor(ancestor);
      continue;
    }

    const encodedPath = joinEncodedKeys(encodedKeys);
    const foundEntry = reader.getClosestEntry(encodedPath);
    if (!foundEntry) {
      const next = null;
      const ancestor = rootCursor;
      const skipped = betweenTreeCursors(ancestor, cursor, next);
      missing = mergeEncodedPathTree(missing, skipped);
      cursor = next;
      continue;
    }
    const [foundPath, foundValue] = foundEntry;
    const value = decodeLeaf(foundValue);

    if (foundPath.compare(encodedPath) === 0) {
      jsonGraph = mergeJsonGraph(
        jsonGraph,
        (jsonGraphFromEncodedEntry(foundPath, value): any)
      );
      cursor = nextTreeCursor(cursor);
      continue;
    }

    const foundKeys = splitEncodedPath(foundPath);
    const { ancestor, remainingPath } = closestCommonAncestorTreeCursor(
      cursor,
      encodedKeys,
      foundKeys
    );

    // short-circuit
    // equivalent to encodedPath.startsWith(foundPath)
    if (!remainingPath) {
      jsonGraph = mergeJsonGraph(
        jsonGraph,
        (jsonGraphFromEncodedEntry(foundPath, value): any)
      );

      if (isRef(value)) {
        // optimize path tree below ancestor onto the ref target.
        const [tree, index] = ancestor;
        const [, subtree] = tree[index];
        const optimized = encodePathTree([(value.value: any)], subtree);
        remaining = mergeEncodedPathTree(remaining, optimized);
      }

      // If value is not a ref we can simply skip over the ancestor's subtree.
      cursor = nextTreeCursor(ancestor);
      continue;
    }

    const next = traverseTreeCursor(ancestor, remainingPath);
    const skipped = betweenTreeCursors(ancestor, cursor, next);
    missing = mergeEncodedPathTree(missing, skipped);
    cursor = next;
    continue;
  }
  return { jsonGraph, remaining, missing };
}

function jsonGraphFromEncodedEntry(
  encodedPath: EncodedPath,
  value: JsonGraphLeaf
): JsonGraphNode {
  return decodePath(encodedPath).reduceRight(
    (acc, cur) => ({ [String(cur)]: acc }),
    value
  );
}

function traverseJsonGraphOnce(
  root: JsonGraph,
  path: Path
): ?{ path: Path, value: JsonGraphLeaf } {
  let branch = root;
  let index = 0;
  while (index < path.length) {
    const key = path[index];
    const value = branch[String(key)];
    if (value === undefined) {
      return null;
    }
    if (isBranch(value)) {
      branch = value;
      continue;
    }
    return {
      path: index === path.length - 1 ? path : path.slice(0, index),
      value
    };
  }
  throw new Error(`branch path requested: ${JSON.stringify(path)}`);
}

class LmdbStorage implements IStorage {
  env: any;
  dbi: any;

  constructor(envOptions: {} = {}, dbiOptions: {} = {}): void {
    this.env = new lmdb.Env();
    this.env.open(envOptions);
    this.dbi = this.env.openDbi({ ...dbiOptions, keyIsBuffer: true });
  }

  getReader(): StorageReader {
    const txn = this.env.beginTxn({ readOnly: true });
    const cursor = new lmdb.Cursor(txn, this.dbi);
    return {
      txn,
      cursor,
      getClosestEntry(encodedPath) {
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

  getWriter(): StorageWriter {
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

  *entries(): Iterable<[EncodedPath, EncodedLeaf]> {
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

interface IStorage {
  getReader(): StorageReader;
  getWriter(): StorageWriter;
  close(): void;
}

type StorageReader = {
  getClosestEntry(encodedPath: EncodedPath): ?[EncodedPath, EncodedLeaf],
  close(): void
};

type StorageWriter = {
  set(encodedPath: EncodedPath, encodedLeaf: EncodedLeaf): void,
  commit(): void,
  abort(): void
};

class CacheDataSource implements IDataSource {
  source: ?IDataSource;
  storage: IStorage;

  constructor(storage: IStorage, source: ?IDataSource): void {
    this.storage = storage;
    this.source = source;
  }

  get(pathSets: PathSet[]): Observable<JsonGraphEnvelope> {
    return Observable.create(observer => {
      const encodedPathTree = encodePathTree(pathSets);
      if (encodedPathTree === null) {
        const envelope = { jsonGraph: {}, paths: [] };
        observer.onNext(envelope);
        observer.onCompleted();
        return;
      }
      const envelope = this._getCache(encodedPathTree);
      if (!envelope.missingPaths || !this.source) {
        observer.onNext(envelope);
        observer.onCompleted();
        return;
      }
      Observable.from(this.source.get(envelope.missingPaths)).subscribe(
        remoteResult => {
          // Ideally we'd pass the result from _setCache into the initial
          // jsonGraph for _getCache but, but different transaction...
          this._setCache(remoteResult);
          const afterWritten = this._getCache(encodedPathTree);
          // should have logic here to get any values that have since expired.
          observer.onNext(afterWritten);
        },
        err => observer.onError(err),
        () => observer.onCompleted()
      );
    });
  }

  _getCache(
    encodedPathTree: EncodedPathTree
  ): JsonGraphEnvelopeWithMissingPaths {
    const reader = this.storage.getReader();
    const envelope = walkCache(encodedPathTree, reader, {});
    reader.close();
    return envelope;
  }

  set(jsonGraphEnvelope: JsonGraphEnvelope): Observable<JsonGraphEnvelope> {
    return Observable.create(observer => {
      const written = this._setCache(jsonGraphEnvelope);
      if (!this.source) {
        observer.onNext(written);
        observer.onCompleted();
        return;
      }
      Observable.from(this.source.set(written)).subscribe(
        remoteResult => observer.onNext(this._setCache(remoteResult)),
        err => observer.onError(err),
        () => observer.onCompleted()
      );
    });
  }

  _setCache(envelope: JsonGraphEnvelope): JsonGraphEnvelope {
    // Ideally this should respect $timestamp metadata and return newer
    const writer = this.storage.getWriter();
    for (const { path, value } of iterJsonGraph(envelope.jsonGraph)) {
      const encodedPath = encodePath(path);
      const encodedValue = encodeLeaf(value);
      writer.set(encodedPath, encodedValue);
    }
    writer.commit();
    return envelope;
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
}

module.exports = { CacheDataSource, LmdbStorage };
