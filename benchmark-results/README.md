# Benchmark Results

This directory contains historical benchmark results for the Imprint Java implementation.

## Files

- `*.json` - Raw JMH benchmark results in JSON format
- `*-summary-*.txt` - Human-readable summaries of benchmark runs
- `system-info-*.txt` - System information for each benchmark run
- `overall-summary-*.txt` - Complete benchmark overview

## Running Benchmarks

### All Benchmarks
```bash
# Unix/Linux/macOS
./run-benchmarks.sh

# Windows
run-benchmarks.bat
```

### Specific Benchmark Categories
```bash
# String performance tests
./gradlew jmh -Pjmh.include=StringBenchmark

# Serialization tests
./gradlew jmh -Pjmh.include=".*[Ss]erial.*"

# Field access tests
./gradlew jmh -Pjmh.include=".*[Aa]ccess.*"

# Comparison tests (vs other formats)
./gradlew jmh -Pjmh.include=ComparisonBenchmark
```

## Analyzing Results

### Online Visualization
Upload JSON files to [JMH Visualizer](https://jmh.morethan.io/) for interactive charts and analysis.

### Command Line Analysis
```bash
# View benchmark names and scores
jq -r '.[] | select(.benchmark) | "\(.benchmark): \(.primaryMetric.score) \(.primaryMetric.scoreUnit)"' results.json

# Find fastest operations
jq -r '.[] | select(.benchmark) | "\(.benchmark): \(.primaryMetric.score)"' results.json | sort -k2 -n

# Compare specific benchmarks
jq '.[] | select(.benchmark | contains("String"))' results.json
```

## Performance Tracking

Results are timestamped and committed to track performance changes over time. Compare results between commits to identify performance regressions or improvements.

## System Requirements

For consistent results:
- Run on dedicated hardware when possible
- Close unnecessary applications
- Run multiple times and compare results
- Note system configuration in commit messages