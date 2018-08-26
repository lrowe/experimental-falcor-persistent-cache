// @flow
"use strict";
/* A TreeCursor immutably represents a position in a SortedPathTree.
 * Use a TreeCursor to iterate over paths in order.
 * [tree, index, parent]
 */
export type TreeCursor = [SortedPathTree, number, ?TreeCursor];

import type { Path } from "falcor-json-graph";
import type { SortedPathTree, SortedIndicesPath } from "./sorted-path-tree";

const { compareKeysEncoded } = require("./encoding");
const {
  aboveLowerBoundExclusive,
  belowUpperBoundInclusive
} = require("./sorted-path-tree");

/* Get a TreeCursor for a SortedPathTree 
 */
function treeCursor(root: SortedPathTree): ?TreeCursor {
  const cursor = [[["", root]], 0, null];
  return firstTreeCursor(cursor);
}

/* Get a cursor to the first leaf node child of current (maybe current)
 */
function firstTreeCursor(current: TreeCursor): ?TreeCursor {
  let [tree, index, parent] = current;
  if (index >= tree.length) {
    if (!parent) {
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
  if (found) {
    return found;
  }
  // This should only happen if subtree is an empty array or it has no
  // non-branch descendants.
  return firstTreeCursor([tree, index + 1, parent]);
}

/* Get a cursor to the first leaf node after current
 */
function nextTreeCursor(current: TreeCursor): ?TreeCursor {
  const [tree, index, parent] = current;
  return firstTreeCursor([tree, index + 1, parent]);
}

/* Return the closest ancestor cursor of current along the path foundPath.
 */
function closestCommonAncestor(
  current: TreeCursor,
  currentPath: Path,
  foundPath: Path
): {
  ancestor: TreeCursor,
  relativePath: ?Path
} {
  const fdi = firstDifferentIndex(foundPath, currentPath);
  let cursor = current;
  let i =
    fdi === -1
      ? currentPath.length - foundPath.length
      : currentPath.length - fdi;
  while (i > 0) {
    const [, , parent] = cursor;
    if (!parent) {
      throw new Error("unreachable");
    }
    cursor = parent;
    --i;
  }
  const ancestor = cursor;
  const relativePath = fdi === -1 ? null : foundPath.slice(fdi);
  return { ancestor, relativePath };
}

/* Return the first leaf cursor beyond the relative path.
 */
function traverseTreeCursor(current: TreeCursor, path: Path): ?TreeCursor {
  if (path.length === 0) {
    return firstTreeCursor(current);
  }
  const [tree, index, parent] = current;
  const [key, ...restPath] = path;
  let i = index;
  while (i < tree.length) {
    const [k, subtree] = tree[i];
    if (compareKeysEncoded(k, key) > 0) {
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

/* Return the SortedPathTree between the current cursor (inclusive) and the
 * next cursor (exclusive).
 */
function sliceTreeBetweenCursors(
  ancestor: TreeCursor,
  current: TreeCursor,
  next: ?TreeCursor
): SortedPathTree | null {
  const [tree, index] = ancestor;
  let [, commonRoot] = tree[index];
  if (commonRoot === null) {
    return null;
  }
  if (next) {
    const lowerIndices = indicesFromTreeCursor(next, ancestor);
    commonRoot = aboveLowerBoundExclusive(commonRoot, lowerIndices);
  }
  if (commonRoot === null) {
    return null;
  }
  const upperIndices = indicesFromTreeCursor(current, ancestor);
  return belowUpperBoundInclusive(commonRoot, upperIndices);
}

/* Return the path between ancestor and cursor.
 */
function pathFromTreeCursor(
  cursor: TreeCursor,
  ancestor: ?TreeCursor = null
): Path {
  const [tree, index, parent] = cursor;
  if (parent === ancestor || !parent) {
    // root cursor
    return [];
  }
  const [key] = tree[index];
  return [...pathFromTreeCursor(parent, ancestor), key];
}

/* Return the indices path between ancestor and cursor.
 */
function indicesFromTreeCursor(
  cursor: TreeCursor,
  ancestor: ?TreeCursor = null
): SortedIndicesPath {
  const [, index, parent] = cursor;
  if (parent === ancestor || !parent) {
    // root cursor
    return [];
  }
  return [...indicesFromTreeCursor(parent, ancestor), index];
}

function firstDifferentIndex(foundPath: Path, searchedPath: Path): number {
  return foundPath.findIndex(
    (k, i) => compareKeysEncoded(searchedPath[i], k) !== 0
  );
}

module.exports = {
  treeCursor,
  firstTreeCursor,
  nextTreeCursor,
  closestCommonAncestor,
  traverseTreeCursor,
  sliceTreeBetweenCursors,
  pathFromTreeCursor,
  indicesFromTreeCursor
};
