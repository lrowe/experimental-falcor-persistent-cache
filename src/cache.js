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
export type EncodedPathTreeEntry = [EncodedKey, EncodedPathTree | null];
export type EncodedPathTree = EncodedPathTreeEntry[];

const { Observable, tap } = require("falcor-observable");
const { mergeJsonGraphEnvelope, mergeJsonGraph } = require("falcor-json-graph");
const { iterKeySet, iterJsonGraph } = require("./iter");
const lmdb = require("node-lmdb");

const EMPTY_BUFFER = Buffer.alloc(0);
const SEP = " "; // all lower chars are escaped.

function encodeKey(key: Key): EncodedKey {
  return encodeURIComponent(String(key));
}

function decodeKey(encoded: EncodedKey): Key {
  return decodeURIComponent(encoded);
}

function encodeKeys(path: Path): EncodedKey[] {
  return path.map(key => encodeKey(key));
}

function joinEncodedKeys(encodedKeys: EncodedKey[]): EncodedPath {
  return encodedKeys.join(SEP) + SEP;
}

function splitEncodedPath(encoded: EncodedPath): EncodedKey[] {
  const out = [];
  let sliceStart = 0;
  let sliceEnd;
  while ((sliceEnd = encoded.indexOf(SEP, sliceStart)) !== -1) {
    out.push(encoded.slice(sliceStart, sliceEnd));
    sliceStart = sliceEnd + 1;
  }
  return out;
}

function encodePath(path: Path): EncodedPath {
  return joinEncodedKeys(encodeKeys(path));
}

function decodePath(encoded: EncodedPath): Path {
  return encoded
    .slice(0, -1)
    .split(SEP)
    .map(e => decodeKey(e));
}

function encodePathTree(pathSet: PathSet): EncodedPathTree | null {
  return pathSet.reduceRight(
    (right, keySet) =>
      Array.from(iterKeySet(keySet))
        .map(key => encodeKey(key))
        .sort()
        .reverse()
        .map(encodedKey => [encodedKey, right]),
    null
  );
}

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
  // Merge maintaining sorted order (highest first).
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

/* Seek to the first tree leaf path less than or equal to foundKeys.
 */
function seekTreeCursor(
  current: TreeCursor,
  foundKeys: EncodedKey[]
): {
  next: TreeCursor | null,
  ancestor: TreeCursor
} {
  const currentKeys = encodedKeysFromTreeCursor(current);
  const fdi = firstDifferentIndex(foundKeys, currentKeys);
  let cursor = current;
  let i = currentKeys.length - fdi;
  while (i > 0) {
    const [, , parent] = cursor;
    if (parent === null) {
      throw new Error("unreachable");
    }
    cursor = parent;
    --i;
  }
  const ancestor = cursor;
  const next = traverseTreeCursor(ancestor, foundKeys.slice(fdi));
  return { next, ancestor };
}

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

function trimUpperInclusive(
  branch: EncodedPathTree,
  indices: number[]
): EncodedPathTree {
  if (indices.length === 0) {
    throw new Error("unreachable");
  }
  const [index, ...remaining] = indices;
  const [key, child] = branch[index];
  if (child === null) {
    return branch.slice(index);
  }
  const newChild = trimUpperInclusive(child, remaining);
  return [[key, newChild], ...branch.slice(index + 1)];
}

function trimLowerExclusive(
  branch: EncodedPathTree,
  indices: number[]
): EncodedPathTree | null {
  if (indices.length === 0) {
    throw new Error("unreachable");
  }
  const [index, ...remaining] = indices;
  const [key, child] = branch[index];
  if (child === null) {
    return branch.slice(0, index);
  }
  const newChild = trimUpperInclusive(child, remaining);
  return [...branch.slice(0, index), [key, newChild]];
}

// inclusive upper bound
// exclusive lower bound
function betweenCursors(
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
    commonRoot = trimLowerExclusive(commonRoot, lowerIndices);
  }
  if (commonRoot === null) {
    return null;
  }
  const upperIndices = indicesFromTreeCursor(current, ancestor);
  return trimUpperInclusive(commonRoot, upperIndices);
}

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

function walkCache(
  pathSets: PathSet[],
  getClosestEntry: (encodedPath: EncodedPath) => [EncodedPath, JsonGraphLeaf]
): {
  found: JsonGraph,
  missing: EncodedPathTree | null
} {
  const encodedPathTree = pathSets
    .map(pathSet => encodePathTree(pathSet))
    .reduce((acc, cur) => mergeEncodedPathTree(acc, cur), null);
  const found = {};
  let missing = null;
  let remaining = encodedPathTree;
  while (remaining !== null) {
    ({ remaining, missing } = _walkCache(
      remaining,
      found,
      missing,
      getClosestEntry
    ));
  }
  return { found, missing };
}

function _walkCache(
  encodedPathTree: EncodedPathTree,
  found: { [encodedPath: EncodedPath]: JsonGraphLeaf },
  missingIn: EncodedPathTree | null,
  getClosestEntry: (encodedPath: EncodedPath) => [EncodedPath, JsonGraphLeaf]
): {
  remaining: EncodedPathTree | null,
  missing: EncodedPathTree | null
} {
  let remaining = null;
  let missing = missingIn;
  let cursor = treeCursor(encodedPathTree);
  while (cursor !== null) {
    const encodedKeys = encodedKeysFromTreeCursor(cursor);
    const encodedPath = joinEncodedKeys(encodedKeys);

    if (typeof found[encodedPath] !== "undefined") {
      cursor = nextTreeCursor(cursor);
      continue;
    }

    const [foundPath, value] = getClosestEntry(encodedPath);

    if (foundPath === encodedPath) {
      found[foundPath] = value;
      cursor = nextTreeCursor(cursor);
      continue;
    }

    const foundKeys = splitEncodedPath(foundPath);
    const fdi = firstDifferentIndex(foundKeys, encodedKeys);

    // short-circuit
    // equivalent to encodedPath.startsWith(foundPath)
    if (fdi === -1) {
      found[foundPath] = value;

      let cursorFound = cursor;
      let i = encodedKeys.length - foundKeys.length;
      while (i > 0 && cursorFound !== null) {
        --i;
        [, , cursorFound] = cursorFound;
      }
      if (cursorFound === null) {
        throw new Error("unreachable");
      }
      const [tree, index] = cursorFound;

      if (
        typeof value === "object" &&
        value !== null &&
        value.$type === "ref"
      ) {
        // follow reference
        const refPath = encodeKeys(value.value);
        const [, seed] = tree[index];
        const optimized = refPath.reduceRight((acc, cur) => {
          return [[cur, acc]];
        }, seed);
        remaining = mergeEncodedPathTree(remaining, optimized);
      }

      cursor = nextTreeCursor(cursorFound);
      continue;
    }

    const { next, ancestor } = seekTreeCursor(cursor, foundKeys);
    const skipped = betweenCursors(ancestor, cursor, next);
    missing = mergeEncodedPathTree(missing, skipped);
    cursor = next;
    continue;
  }
  return { remaining, missing };
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
      const jsonGraph = Object.entries(found)
        .map(([encoded, value]) =>
          jsonGraphFromEncodedEntry(encoded, (value: any))
        )
        .reduce((acc, cur) => mergeJsonGraph(acc, (cur: any)), {});
      let envelope = { jsonGraph };
      if (missing === null || !this.source) {
        observer.onNext(envelope);
        observer.onCompleted();
        return;
      }
      this.source.get((missing: any)).subscribe(
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

module.exports = { CacheDataSource };
