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
export type EncodedPath = Buffer;
export type EncodedLeaf = Buffer;
export type EncodedPathTreeEntry = [EncodedKey, EncodedPathTree | null];
export type EncodedPathTree = EncodedPathTreeEntry[];

const { Observable, tap } = require("falcor-observable");
const { mergeJsonGraphEnvelope, mergeJsonGraph } = require("falcor-json-graph");
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
  pathSets: PathSet[],
  getClosestEntry: (encodedPath: EncodedPath) => ?[EncodedPath, EncodedLeaf]
): {
  found: JsonGraph,
  missing: EncodedPathTree | null
} {
  const encodedPathTree = encodePathTree(pathSets);
  let found = {};
  let missing = null;
  let remaining = encodedPathTree;
  while (remaining !== null) {
    ({ remaining, missing, found } = _walkCache(
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
  foundIn: JsonGraph,
  missingIn: EncodedPathTree | null,
  getClosestEntry: (encodedPath: EncodedPath) => ?[EncodedPath, EncodedLeaf]
): {
  remaining: EncodedPathTree | null,
  missing: EncodedPathTree | null,
  found: JsonGraph
} {
  let found = foundIn;
  let remaining = null;
  let missing = missingIn;
  const rootCursor = treeCursor(encodedPathTree);
  if (rootCursor === null) {
    return { remaining, missing, found };
  }
  let cursor = rootCursor;
  while (cursor !== null) {
    const encodedKeys = encodedKeysFromTreeCursor(cursor);

    // First look in our already found jsonGraph
    const cursorPath = decodeKeys(encodedKeys);
    const foundPV = traverseJsonGraphOnce(found, cursorPath);
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
    const foundEntry = getClosestEntry(encodedPath);
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
      found = mergeJsonGraph(
        found,
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
      found = mergeJsonGraph(
        found,
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
  return { remaining, missing, found };
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
      const jsonGraph = found;
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
  ): ?[EncodedPath, EncodedLeaf] {
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

  _setCache({ jsonGraph }: JsonGraphEnvelope): void {
    const txn = this.env.beginTxn();
    for (const { path, value } of iterJsonGraph(jsonGraph)) {
      const encodedPath = encodePath(path);
      const encodedValue = encodeLeaf(value);
      txn.putBinary(this.dbi, encodedPath, encodedValue);
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
      yield found;
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
