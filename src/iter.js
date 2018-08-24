// @flow
"use strict";
import type {
  Key,
  KeySet,
  Path,
  PathSet,
  JsonGraphNode,
  JsonGraphLeaf
} from "falcor-json-graph";

function* iterKeySet(keySet: KeySet): Iterable<Key> {
  if (typeof keySet !== "object" || keySet === null) {
    yield keySet;
    return;
  }
  if (Array.isArray(keySet)) {
    for (const keyOrKeyRange of keySet) {
      yield* iterKeySet(keyOrKeyRange);
    }
    return;
  }
  const { from = 0, to = keySet.length + from - 1 } = keySet;
  for (let i = from; i <= to; i++) {
    yield i;
  }
}

function* iterPathSet(pathSet: PathSet): Iterable<Path> {
  if (pathSet.length === 0) {
    yield [];
    return;
  }
  const [keySet, ...tail] = pathSet;
  for (const key of iterKeySet(keySet)) {
    for (const path of iterPathSet(tail)) {
      yield [key, ...path];
    }
  }
}

function* iterJsonGraph(
  value: JsonGraphNode,
  path: Path = []
): Iterable<{ path: Path, value: JsonGraphLeaf }> {
  if (value === null || typeof value !== "object" || value.$type) {
    yield { path, value };
  } else {
    for (const key of Object.keys(value)) {
      const child = value[key];
      if (child !== undefined) {
        yield* iterJsonGraph(child, path.concat(key));
      }
    }
  }
}

module.exports = { iterKeySet, iterPathSet, iterJsonGraph };
