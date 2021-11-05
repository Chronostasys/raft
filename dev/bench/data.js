window.BENCHMARK_DATA = {
  "lastUpdate": 1636104930944,
  "repoUrl": "https://github.com/Chronostasys/raft",
  "entries": {
    "Benchmark": [
      {
        "commit": {
          "author": {
            "email": "1769712655@qq.com",
            "name": "Chronos",
            "username": "Chronostasys"
          },
          "committer": {
            "email": "1769712655@qq.com",
            "name": "Chronos",
            "username": "Chronostasys"
          },
          "distinct": true,
          "id": "9bae6e65694a03531077c97b75d7141ba661e340",
          "message": "add benchmark ci",
          "timestamp": "2021-11-05T17:07:21+08:00",
          "tree_id": "68d3de5002c5da817dbbac91d8ad3c18584758a4",
          "url": "https://github.com/Chronostasys/raft/commit/9bae6e65694a03531077c97b75d7141ba661e340"
        },
        "date": 1636103983832,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkGet",
            "value": 177214,
            "unit": "ns/op",
            "extra": "10070 times\n2 procs"
          },
          {
            "name": "BenchmarkPut",
            "value": 127744,
            "unit": "ns/op",
            "extra": "10210 times\n2 procs"
          },
          {
            "name": "BenchmarkAppend",
            "value": 1463075,
            "unit": "ns/op",
            "extra": "9904 times\n2 procs"
          }
        ]
      },
      {
        "commit": {
          "author": {
            "email": "1769712655@qq.com",
            "name": "Chronos",
            "username": "Chronostasys"
          },
          "committer": {
            "email": "1769712655@qq.com",
            "name": "Chronos",
            "username": "Chronostasys"
          },
          "distinct": true,
          "id": "213d2823031c304d8ad27d17a1b36f2f33f9e25f",
          "message": "fix bench",
          "timestamp": "2021-11-05T17:23:20+08:00",
          "tree_id": "d51927fd10ba44a547c409d5fe18f737b5237879",
          "url": "https://github.com/Chronostasys/raft/commit/213d2823031c304d8ad27d17a1b36f2f33f9e25f"
        },
        "date": 1636104930012,
        "tool": "go",
        "benches": [
          {
            "name": "BenchmarkGet",
            "value": 523792,
            "unit": "ns/op",
            "extra": "6372 times\n2 procs"
          },
          {
            "name": "BenchmarkPut",
            "value": 1442960,
            "unit": "ns/op",
            "extra": "6991 times\n2 procs"
          },
          {
            "name": "BenchmarkAppend",
            "value": 233172,
            "unit": "ns/op",
            "extra": "12984 times\n2 procs"
          }
        ]
      }
    ]
  }
}