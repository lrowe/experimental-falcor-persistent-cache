// @flow
"use strict";

import type {
  JsonGraph,
  JsonGraphNode,
  JsonGraphLeaf,
  Path,
  PathSet
} from "falcor-json-graph";

export type JsonGraphEnvelopeWithMissingPaths = {
  jsonGraph: JsonGraph,
  paths?: PathSet[],
  missingPaths?: PathSet[],
  invalidated?: PathSet[],
  context?: JsonGraph
};

function isRef(node: JsonGraphNode): boolean %checks {
  return typeof node === "object" && node !== null && node.$type === "ref";
}

function isBranch(node: JsonGraphNode): boolean %checks {
  return typeof node === "object" && node !== null && node.$type === undefined;
}

function jsonGraphFromPathValue(path: Path, value: JsonGraphLeaf): JsonGraph {
  const node = path.reduceRight((acc, cur) => ({ [String(cur)]: acc }), value);
  if (!isBranch(node)) {
    throw new Error("should not reach here");
  }
  return node;
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

module.exports = {
  isRef,
  isBranch,
  jsonGraphFromPathValue,
  traverseJsonGraphOnce
};
