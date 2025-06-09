namespace java com.imprint.benchmark.thrift

struct TestRecord {
  1: required string id;
  2: required i64 timestamp;
  3: required i32 flags;
  4: required bool active;
  5: required double value;
  6: required binary data;
  7: required list<i32> tags;
  8: required map<string, string> metadata;
}

struct ProjectedRecord {
  1: required string id;
  2: required i64 timestamp;
  3: required list<i32> tags;
} 