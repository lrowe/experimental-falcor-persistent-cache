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

const { Observable, map } = require("falcor-observable");
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
  sortedPathTree: SortedPathTree,
  reader: IStorageReader,
  initialJsonGraph: JsonGraph
): JsonGraphEnvelopeWithMissingPaths {
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
    const value = decodeValue(foundEncodedValue);

    if (foundEncodedPath.compare(encodedPath) === 0) {
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

  get(pathSets: PathSet[]): Observable<JsonGraphEnvelope> {
    return Observable.create(observer => {
      const sortedPathTree = sortedPathTreeFromPathSets(pathSets);
      const envelope = this._getCache(sortedPathTree);
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
          const afterWritten = this._getCache(sortedPathTree);
          // should have logic here to get any values that have since expired.
          observer.onNext(afterWritten);
        },
        err => observer.onError(err),
        () => observer.onCompleted()
      );
    });
  }

  _getCache(
    sortedPathTree: ?SortedPathTree
  ): JsonGraphEnvelopeWithMissingPaths {
    if (!sortedPathTree) {
      return { jsonGraph: {}, paths: [] };
    }
    const reader = this.storage.getReader();
    const envelope = walkCache(sortedPathTree, reader, {});
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
      const encodedValue = encodeValue(value);
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
    ).pipe(map(remoteValue => this._setCache(remoteValue)));
  }
}

module.exports = { CacheDataSource };
