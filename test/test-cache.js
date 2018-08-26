// @flow
"use strict";
const { expect } = require("chai");
const { CacheDataSource, LmdbStorage } = require("../src/cache");
const tmp = require("tmp");

function range(start: number, stop?: number): number[] {
  if (stop === undefined) {
    stop = start;
    start = 0;
  }
  return Array(stop - start)
    .fill(start)
    .map((x, i) => x + i);
}

function makeLolomo(rows: number = 1, cols: number = 1, id: string = "ABC") {
  const lolomo = { $type: "ref", value: ["lolomos", id] };
  const lolomos = {
    [id]: range(rows)
      .map(i => ({ $type: "ref", value: ["lists", `${id}${i}`] }))
      .reduce((acc, v, i) => ({ ...acc, [i]: v }), {})
  };
  const lists = range(rows)
    .map(i =>
      range(cols)
        .map(j => ({ $type: "ref", value: ["videos", i * cols + j] }))
        .reduce((acc, v, j) => ({ ...acc, [j]: v }), {})
    )
    .reduce((acc, v, i) => ({ ...acc, [`${id}${i}`]: v }), {});
  const videos = range(rows * cols)
    .map(x => ({ title: `title ${Math.trunc(x / cols)} ${x % cols}` }))
    .reduce((acc, v, x) => ({ ...acc, [x]: v }), {});
  const jsonGraph = { lolomo, lolomos, lists, videos };
  const paths = [["lolomo", { length: ROWS }, { length: COLS }, "title"]];
  return { jsonGraph, paths };
}

const ROWS = 2;
const COLS = 2;
const basicLolomo = makeLolomo(ROWS, COLS);

describe("CacheDataSource", function() {
  let tmpdir;
  let storage;
  let ds;

  beforeEach(function() {
    tmpdir = tmp.dirSync({ unsafeCleanup: true });
    storage = new LmdbStorage(
      { path: tmpdir.name },
      { name: "mydb", create: true }
    );
    ds = new CacheDataSource(storage);
  });

  afterEach(function() {
    tmpdir.removeCallback();
    storage.close();
  });

  it("sets data", function(done) {
    ds.set(basicLolomo).subscribe(null, done, () => {
      const keys = Array.from(storage.keys());
      expect(keys.length).to.equal(1 + ROWS + ROWS * COLS + ROWS * COLS);
      done();
    });
  });

  it("gets data", function(done) {
    ds.set(basicLolomo).subscribe();
    ds.get(basicLolomo.paths).subscribe(
      result => {
        expect(result.jsonGraph).to.deep.equal(basicLolomo.jsonGraph);
      },
      done,
      done
    );
  });
});
