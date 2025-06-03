# Performance Profiling Guide

This guide helps developers identify performance hotspots in the Imprint Java implementation.

## Quick Start

```bash
# Run field access profiling with async-profiler
./profile.sh profileFieldAccess asyncprofiler

# Run memory allocation profiling with JFR
./profile.sh profileMemoryAllocation jfr
```

## Available Tests

1. **`profileFieldAccess`** - Measures random field access patterns
   - Focus: Binary search, TypeHandler dispatch, string decoding
   - Good for: Optimizing read-heavy workloads

2. **`profileSerialization`** - Tests record creation and serialization
   - Focus: Object allocation, ByteBuffer operations, encoding
   - Good for: Optimizing write-heavy workloads

3. **`profileProjection`** - Simulates analytical field projection
   - Focus: Bulk field access, string materialization
   - Good for: Optimizing analytical workloads

4. **`profileMemoryAllocation`** - Stress tests allocation patterns
   - Focus: GC pressure, object lifecycle, string allocations
   - Good for: Reducing memory footprint

## Profiler Options

### async-profiler (Recommended)
- **Setup**: Download from [async-profiler releases](https://github.com/jvm-profiling-tools/async-profiler/releases)
- **Output**: HTML flame graphs in `profiler-results/`
- **Best for**: CPU profiling, finding hot methods

### Java Flight Recorder (JFR)
- **Setup**: Built into OpenJDK 11+
- **Output**: `.jfr` files for Java Mission Control
- **Best for**: Memory profiling, GC analysis

### VisualVM
- **Setup**: `jvisualvm` (usually pre-installed)
- **Output**: Real-time profiling UI
- **Best for**: Interactive profiling, heap dumps

## Expected Hotspots

Based on our optimizations, watch for:

### CPU Hotspots
1. **Binary search** in `findDirectoryIndex()` - should be fast
2. **String decoding** in `StringBufferValue.getValue()` - lazy evaluation
3. **TypeHandler dispatch** - interface calls vs switch statements
4. **VarInt encoding/decoding** - variable-length integers
5. **ByteBuffer operations** - slicing and positioning

### Memory Hotspots
1. **String allocations** during UTF-8 conversion
2. **Temporary objects** in binary search (should be eliminated)
3. **ByteBuffer slicing** (should be zero-copy)
4. **Array allocations** for BYTES values

## Analyzing Results

### async-profiler Flame Graphs
- **Wide bars** = high CPU usage (hotspots)
- **Deep stacks** = call overhead
- **Look for**: Red bars in `deserializeValue`, `findDirectoryIndex`, string operations

### JFR Analysis
1. Open `.jfr` file in Java Mission Control
2. Check "Memory" tab for allocation hotspots
3. Check "Method Profiling" for CPU usage
4. Look at GC events for memory pressure

### Memory Profiler Tips
- **Object allocation rate** should be low for zero-copy operations
- **String allocations** should be rare (lazy evaluation)
- **GC frequency** indicates allocation pressure

## Performance Targets

Based on our benchmarks:
- **Single field access**: < 50ns
- **Zero-copy operations**: < 30ns  
- **String decoding**: Should be lazy, not in hot path
- **Binary search**: O(log n), ~10ns per comparison

## Common Issues

1. **High string allocation** → Enable lazy string decoding
2. **Object allocations in binary search** → Check DirectoryEntry creation
3. **ByteBuffer copying** → Ensure zero-copy slicing
4. **Switch statement overhead** → TypeHandler dispatch working?

## Profiling Best Practices

1. **Warm up JVM** - Run tests multiple times
2. **Use realistic data** - Match production patterns  
3. **Profile different scenarios** - Read vs write heavy
4. **Check allocations** - Memory profiling reveals hidden costs
5. **Compare before/after** - Measure optimization impact