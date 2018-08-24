// @flow
"use strict";
const { expect } = require("chai");
const { CacheDataSource } = require("../src/cache");
const tmp = require("tmp");

const basicLolomo = {
  jsonGraph: {
    lolomo: { $type: "ref", value: ["lolomos", "ABC"] },
    lolomos: {
      ABC: {
        "0": { $type: "ref", value: ["lists", "ABC0"] }
      }
    },
    lists: {
      ABC0: {
        "0": { $type: "ref", value: ["videos", 123] }
      }
    },
    videos: {
      "123": {
        title: "Stranger Things"
      }
    }
  }
};

describe("CacheDataSource", function() {
  let tmpdir;

  beforeEach(function() {
    tmpdir = tmp.dirSync({ unsafeCleanup: true });
  });

  afterEach(function() {
    tmpdir.removeCallback();
  });

  it("sets data", function() {
    const ds = new CacheDataSource(
      null,
      { path: tmpdir.name },
      { name: "mydb", create: true }
    );
    ds._setCache(basicLolomo);
    const keys = Array.from(ds.keys());
    expect(keys.length).to.equal(4);
  });

  it("gets data", function(done) {
    const ds = new CacheDataSource(
      null,
      { path: tmpdir.name },
      { name: "mydb", create: true }
    );
    ds._setCache(basicLolomo);
    ds.get([["lolomo", 0, 0, "title"]]).subscribe(
      result => {
        expect(result.jsonGraph).to.deep.equal(basicLolomo.jsonGraph);
      },
      done,
      done
    );
  });
});
