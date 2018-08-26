// @flow
"use strict";

/* A SortedPathTree is an immutable, sorted, representation of paths.
 * See TreeCursors for operations over
 */
export type SortedPathTree = SortedPathTreeEntry[];
export type SortedPathTreeEntry = [Key, SortedPathTree | null];

/* A path along a SortedPathTree by sorted index.
 */
export type SortedIndicesPath = number[];

import type { PathTree, LengthTree } from "falcor-path-utils";
import type { Key, PathSet } from "falcor-json-graph";

const { toPaths } = require("falcor-path-utils");
const { iterKeySet } = require("./iter");
const { compareKeysEncoded } = require("./encoding");

/* Return SortedPathTree for pathSets.
 * Supply subtree when optimizing an existing SortedPathTree.
 */
function sortedPathTreeFromPathSets(
  pathSets: PathSet[],
  subtree: SortedPathTree | null = null
): SortedPathTree | null {
  return pathSets
    .map(pathSet =>
      pathSet.reduceRight(
        (right, keySet) =>
          Array.from(iterKeySet(keySet))
            .sort(compareKeysEncoded)
            .reverse()
            .map(key => [key, right]),
        subtree
      )
    )
    .reduce((acc, cur) => mergeSortedPathTree(acc, cur), null);
}

/* Immutably merge SortedPathTrees maintaining sorted order (highest first).
 */
function mergeSortedPathTree(
  left: SortedPathTree | null,
  right: SortedPathTree | null
): SortedPathTree | null {
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
      (rightEntry === undefined ||
        compareKeysEncoded(leftEntry[0], rightEntry[0]) > 0)
    ) {
      merged.push(leftEntry);
      leftIndex++;
      leftEntry = left[leftIndex];
    }
    while (
      rightEntry !== undefined &&
      (leftEntry === undefined ||
        compareKeysEncoded(rightEntry[0], leftEntry[0]) > 0)
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
      const value = mergeSortedPathTree(leftEntry[1], rightEntry[1]);
      merged.push([key, value]);
      leftIndex++;
      leftEntry = left[leftIndex];
      rightIndex++;
      rightEntry = right[rightIndex];
    }
  }
  return merged;
}

/* Return PathSets for SortedPathTree
 */
function pathSetsFromSortedPathTree(sortedPathTree: SortedPathTree): PathSet[] {
  return toPaths(lengthTreeFromSortedPathTree(sortedPathTree));
}

function lengthTreeFromSortedPathTree(
  sortedPathTree: SortedPathTree
): LengthTree {
  const lengthTree = {};
  _lengthTreeFromSortedPathTree(lengthTree, sortedPathTree, []);
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

function _lengthTreeFromSortedPathTree(
  lengthTree: LengthTree,
  sortedPathTree: SortedPathTree,
  prefix: string[]
): void {
  let lengthTreeNode;
  for (const [key, child] of sortedPathTree) {
    if (child === null) {
      if (lengthTreeNode === undefined) {
        lengthTreeNode = _lengthTreeParentNode(lengthTree, prefix);
      }
      lengthTreeNode[String(key)] = null;
    } else {
      _lengthTreeFromSortedPathTree(lengthTree, child, [
        ...prefix,
        String(key)
      ]);
    }
  }
}

/* Return the SortedPathTree without the section above indicesPath
 * (including the indicesPath bound itself.)
 */
function belowUpperBoundInclusive(
  branch: SortedPathTree,
  indicesPath: SortedIndicesPath
): SortedPathTree {
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

/* Return the SortedPathTree without the section below indicesPath
 * (excluding the indicesPath bound itself.)
 */
function aboveLowerBoundExclusive(
  branch: SortedPathTree,
  indicesPath: SortedIndicesPath
): SortedPathTree | null {
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

module.exports = {
  sortedPathTreeFromPathSets,
  mergeSortedPathTree,
  pathSetsFromSortedPathTree,
  lengthTreeFromSortedPathTree,
  belowUpperBoundInclusive,
  aboveLowerBoundExclusive
};
