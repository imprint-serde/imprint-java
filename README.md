# imprint-java
Java implementation of the Imprint Serde

## Payload Encoding

| `type_code` | Type       | Encoding details                                       |
| ----------: | ---------- | ------------------------------------------------------ |
|         0x0 | `null`     | No payload; `length` = 0                               |
|         0x1 | `bool`     | 1 byte `0x00` / `0x01`                                 |
|         0x2 | `int32`    | 4-byte signed int32                                    |
|         0x3 | `int64`    | 8-byte signed int64                                    |
|         0x4 | `float32`  | IEEE‑754 little‑endian bytes                           |
|         0x5 | `float64`  | IEEE‑754 little‑endian bytes                           |
|         0x6 | `bytes`    | `length` + payload                                     |
|         0x7 | `string`   | UTF‑8, `length` + payload                              |
|         0x8 | `array`    | `size` + `type_code` + payload                         |
|         0x9 | `map`      | `size` + `key_type_code` + `value_type_code` + payload |
|         0xA | `row`      | Nested Imprint row (recursive joins)                   |
|         0xB | `date`     | 4-byte int32 days since Unix epoch                     |
|         0xC | `time`     | 4-byte int32 milliseconds since midnight               |
|         0xD | `timestamp`| 4-byte int32 milliseconds since Unix epoch (UTC)      |
|         0xE | `uuid`     | 16-byte binary representation                          |
|         0xF | `decimal`  | VarInt scale + VarInt length + unscaled bytes          |
|      16–127 | *reserved* | Future primitives / logical types                      |
