// @flow
"use strict";
import type { Key, Path, JsonGraphLeaf } from "falcor-json-graph";
export type EncodedPath = Buffer;
export type EncodedValue = Buffer;

type EncodedKey = string;

const SEP = " "; // all lower chars are escaped.
const SEP_BUFFER = Buffer.from(SEP);

function encodeKey(key: Key): EncodedKey {
  return encodeURIComponent(String(key));
}

function decodeKey(encoded: EncodedKey): Key {
  return decodeURIComponent(encoded);
}

function encodePath(path: Path): EncodedPath {
  const out = [];
  for (const key of path) {
    out.push(Buffer.from(encodeKey(key)), SEP_BUFFER);
  }
  return Buffer.concat(out);
}

function decodePath(encoded: EncodedPath): Path {
  const out = [];
  let sliceStart = 0;
  let sliceEnd;
  while ((sliceEnd = encoded.indexOf(SEP_BUFFER, sliceStart)) !== -1) {
    out.push(decodeKey(encoded.slice(sliceStart, sliceEnd).toString()));
    sliceStart = sliceEnd + 1;
  }
  return out;
}

function encodeValue(value: JsonGraphLeaf): EncodedValue {
  return Buffer.from(JSON.stringify(value));
}

function decodeValue(encoded: EncodedValue): JsonGraphLeaf {
  return JSON.parse(encoded.toString());
}

function compareKeysEncoded(left: Key, right: Key): number {
  const a = encodeKey(left);
  const b = encodeKey(right);
  return a > b ? 1 : a < b ? -1 : 0;
}

module.exports = {
  encodePath,
  decodePath,
  encodeValue,
  decodeValue,
  compareKeysEncoded
};
