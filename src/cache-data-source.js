// @flow
"use strict";
import type {
  JsonGraph,
  JsonGraphEnvelope,
  JsonGraphNode,
  Path,
  PathSet,
  IDataSource
} from "falcor-json-graph";

import type { JsonGraphEnvelopeWithMissingPaths } from "./util";
import type { SortedPathTree } from "./sorted-path-tree";
import type { IStorage, IStorageReader } from "./storage";

const { Observable, map, mergeMap } = require("falcor-observable");
const { mergeJsonGraph } = require("falcor-json-graph");
const { iterJsonGraph } = require("./iter");
const {
  isRef,
  jsonGraphFromPathValue,
  traverseJsonGraphOnce
} = require("./util");
const {
  sortedPathTreeFromPathSets,
  mergeSortedPathTree,
  pathSetsFromSortedPathTree
} = require("./sorted-path-tree");
const {
  treeCursor,
  pathFromTreeCursor,
  nextTreeCursor,
  closestCommonAncestor,
  sliceTreeBetweenCursors,
  traverseTreeCursor
} = require("./tree-cursor");
const {
  encodePath,
  decodePath,
  encodeValue,
  decodeValue
} = require("./encoding");

function walkCache(
  sortedPathTree: ?SortedPathTree,
  reader: IStorageReader,
  initialJsonGraph: JsonGraph = {}
): JsonGraphEnvelopeWithMissingPaths {
  if (!sortedPathTree) {
    return { jsonGraph: {}, paths: [] };
  }
  let jsonGraph = initialJsonGraph;
  let missing = null;
  let remaining = sortedPathTree;
  while (remaining !== null) {
    ({ remaining, missing, jsonGraph } = walkCacheOnce(
      remaining,
      jsonGraph,
      missing,
      reader
    ));
  }
  if (missing) {
    const missingPaths = pathSetsFromSortedPathTree(missing);
    return { jsonGraph, missingPaths };
  }
  // XXX could probably set paths here too.
  return { jsonGraph };
}

function walkCacheOnce(
  sortedPathTree: SortedPathTree,
  initialJsonGraph: JsonGraph,
  missingIn: SortedPathTree | null,
  reader: IStorageReader
): {
  jsonGraph: JsonGraph,
  remaining: SortedPathTree | null,
  missing: SortedPathTree | null
} {
  let jsonGraph = initialJsonGraph;
  let remaining = null;
  let missing = missingIn;
  const rootCursor = treeCursor(sortedPathTree);
  if (!rootCursor) {
    return { jsonGraph, remaining, missing };
  }
  let cursor = rootCursor;
  while (cursor) {
    const path = pathFromTreeCursor(cursor);

    // First look in our already found jsonGraph
    const foundPV = traverseJsonGraphOnce(jsonGraph, path);
    if (foundPV) {
      const { path: foundPath, value } = foundPV;

      if (path.length === foundPath.length) {
        cursor = nextTreeCursor(cursor);
        continue;
      }

      const { ancestor } = closestCommonAncestor(cursor, path, foundPath);

      if (isRef(value)) {
        // optimize path tree below ancestor onto the ref target.
        const [tree, index] = ancestor;
        const [, subtree] = tree[index];
        const optimized = sortedPathTreeFromPathSets(
          [(value.value: any)],
          subtree
        );
        remaining = mergeSortedPathTree(remaining, optimized);
      }

      // If value is not a ref we can simply skip over the ancestor's subtree.
      cursor = nextTreeCursor(ancestor);
      continue;
    }

    const encodedPath = encodePath(path);
    const foundEntry = reader.getLessThanEqual(encodedPath);
    if (!foundEntry) {
      const next = null;
      const ancestor = rootCursor;
      const skipped = sliceTreeBetweenCursors(ancestor, cursor, next);
      missing = mergeSortedPathTree(missing, skipped);
      cursor = next;
      continue;
    }
    const [foundEncodedPath, foundEncodedValue] = foundEntry;
    const foundPath = decodePath(foundEncodedPath);

    if (foundEncodedPath.compare(encodedPath) === 0) {
      const value = decodeValue(foundEncodedValue);
      jsonGraph = mergeJsonGraph(
        jsonGraph,
        jsonGraphFromPathValue(foundPath, value)
      );
      cursor = nextTreeCursor(cursor);
      continue;
    }

    const { ancestor, relativePath } = closestCommonAncestor(
      cursor,
      path,
      foundPath
    );

    // short-circuit
    // equivalent to encodedPath.startsWith(foundPath)
    if (!relativePath) {
      const value = decodeValue(foundEncodedValue);
      jsonGraph = mergeJsonGraph(
        jsonGraph,
        jsonGraphFromPathValue(foundPath, value)
      );

      if (isRef(value)) {
        // optimize path tree below ancestor onto the ref target.
        const [tree, index] = ancestor;
        const [, subtree] = tree[index];
        const optimized = sortedPathTreeFromPathSets(
          [(value.value: any)],
          subtree
        );
        remaining = mergeSortedPathTree(remaining, optimized);
      }

      // If value is not a ref we can simply skip over the ancestor's subtree.
      cursor = nextTreeCursor(ancestor);
      continue;
    }

    const next = traverseTreeCursor(ancestor, relativePath);
    const skipped = sliceTreeBetweenCursors(ancestor, cursor, next);
    missing = mergeSortedPathTree(missing, skipped);
    cursor = next;
    continue;
  }
  return { jsonGraph, remaining, missing };
}

class CacheDataSource implements IDataSource {
  source: ?IDataSource;
  storage: IStorage;

  constructor(storage: IStorage, source: ?IDataSource): void {
    this.storage = storage;
    this.source = source;
  }

  get(pathSets: PathSet[]): Observable<JsonGraphEnvelopeWithMissingPaths> {
    const sortedPathTree = sortedPathTreeFromPathSets(pathSets);
    return Observable.from(this.storage.getReader()).pipe(
      map(reader => walkCache(sortedPathTree, reader)),
      mergeMap(envelope => {
        if (!envelope.missingPaths || !this.source) {
          return Observable.of(envelope);
        }
        return Observable.from(this.source.get(envelope.missingPaths)).pipe(
          mergeMap(remoteResult =>
            // should have logic here to get any values that have since expired.
            this._setAndGetCache(remoteResult, sortedPathTree)
          )
        );
      })
    );
  }

  set(
    jsonGraphEnvelope: JsonGraphEnvelope
  ): Observable<JsonGraphEnvelopeWithMissingPaths> {
    const { jsonGraph, paths } = jsonGraphEnvelope;
    const sortedPathTree = paths ? sortedPathTreeFromPathSets(paths) : null;
    const obs = this._setAndGetCache(jsonGraph, sortedPathTree);
    const { source } = this;
    if (!source) {
      return obs;
    }
    return obs.pipe(
      mergeMap(written =>
        Observable.from(source.set(written)).pipe(
          mergeMap(remoteResult =>
            this._setAndGetCache(remoteResult.jsonGraph, sortedPathTree)
          )
        )
      )
    );
  }

  _setAndGetCache(
    jsonGraph: JsonGraph,
    sortedPathTree: ?SortedPathTree
  ): Observable<JsonGraphEnvelopeWithMissingPaths> {
    return Observable.from(this.storage.getReaderWriter()).pipe(
      map(rw => {
        // Ideally this should respect $timestamp metadata and return newer
        for (const { path, value } of iterJsonGraph(jsonGraph)) {
          const encodedPath = encodePath(path);
          const encodedValue = encodeValue(value);
          rw.setPathValue(encodedPath, encodedValue);
        }
        return walkCache(sortedPathTree, rw, jsonGraph);
      })
    );
  }

  call(
    callPath: Path,
    args?: JsonGraphNode[] = [],
    refPaths?: PathSet[] = [],
    thisPaths?: PathSet[] = []
  ): Observable<JsonGraphEnvelopeWithMissingPaths> {
    const { source } = this;
    if (!source) {
      throw new Error("must have a source for call");
    }
    return Observable.from(
      source.call(callPath, args, refPaths, thisPaths)
    ).pipe(
      mergeMap(remoteValue => {
        const { paths } = remoteValue;
        const sortedPathTree = paths ? sortedPathTreeFromPathSets(paths) : null;
        return this._setAndGetCache(remoteValue, sortedPathTree);
      })
    );
  }
}

module.exports = { CacheDataSource };
