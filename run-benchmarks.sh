#!/bin/bash

# Benchmark execution script for Imprint Java implementation
# This script runs all benchmark suites and saves results with timestamps

set -e

TIMESTAMP=$(date '+%Y-%m-%d-%H%M%S')
RESULTS_DIR="./benchmark-results"
SYSTEM_INFO_FILE="$RESULTS_DIR/system-info-$TIMESTAMP.txt"

echo "🏃 Running Imprint Java Benchmarks - $TIMESTAMP"
echo "================================================"

# Ensure results directory exists
mkdir -p "$RESULTS_DIR"

# Capture system information
echo "📊 Capturing system information..."
{
    echo "Benchmark Run: $TIMESTAMP"
    echo "=============================="
    echo ""
    echo "System Information:"
    echo "- Date: $(date)"
    echo "- OS: $(uname -a)"
    echo "- Java Version:"
    java -version 2>&1 | sed 's/^/  /'
    echo ""
    echo "- Gradle Version:"
    ./gradlew --version | sed 's/^/  /'
    echo ""
    echo "- Git Commit:"
    echo "  - Hash: $(git rev-parse HEAD)"
    echo "  - Branch: $(git rev-parse --abbrev-ref HEAD)"
    echo "  - Date: $(git log -1 --format=%cd)"
    echo "  - Message: $(git log -1 --format=%s)"
    echo ""
    echo "Hardware Information:"
    if command -v lscpu &> /dev/null; then
        echo "- CPU:"
        lscpu | grep -E "Model name|Architecture|CPU\(s\)|Thread|Core" | sed 's/^/  /'
    fi
    if command -v free &> /dev/null; then
        echo "- Memory:"
        free -h | sed 's/^/  /'
    fi
    echo ""
} > "$SYSTEM_INFO_FILE"

echo "✅ System info saved to: $SYSTEM_INFO_FILE"

# Function to run a specific benchmark suite
run_benchmark_suite() {
    local suite_name=$1
    local suite_pattern=$2
    local output_file="$RESULTS_DIR/${suite_name}-$TIMESTAMP.json"
    
    echo "🔄 Running $suite_name benchmarks..."
    echo "   Pattern: $suite_pattern"
    echo "   Output: $output_file"
    
    ./gradlew jmh \
        -Pjmh.include="$suite_pattern" \
        -Pjmh.resultsFile="$output_file" \
        --console=plain
    
    if [ -f "$output_file" ]; then
        echo "✅ $suite_name completed: $output_file"
        
        # Generate a human-readable summary
        local summary_file="$RESULTS_DIR/${suite_name}-summary-$TIMESTAMP.txt"
        {
            echo "$suite_name Benchmark Summary - $TIMESTAMP"
            echo "======================================="
            echo ""
            echo "Top 10 Fastest Operations:"
            jq -r '.[] | select(.benchmark) | "\(.benchmark): \(.primaryMetric.score | tonumber | . * 1000000 | floor / 1000000) \(.primaryMetric.scoreUnit)"' "$output_file" | sort -k2 -n | head -10
            echo ""
            echo "Top 10 Slowest Operations:"
            jq -r '.[] | select(.benchmark) | "\(.benchmark): \(.primaryMetric.score | tonumber | . * 1000000 | floor / 1000000) \(.primaryMetric.scoreUnit)"' "$output_file" | sort -k2 -nr | head -10
            echo ""
        } > "$summary_file" 2>/dev/null || echo "⚠️  Could not generate summary (jq not available)"
    else
        echo "❌ $suite_name failed - no output file generated"
    fi
    echo ""
}

# Run all benchmark suites
echo "🚀 Starting benchmark execution..."
echo ""

# 1. Serialization benchmarks
run_benchmark_suite "serialization" ".*[Ss]erial.*"

# 2. Field access benchmarks  
run_benchmark_suite "field-access" ".*[Aa]ccess.*"

# 3. Merge benchmarks
run_benchmark_suite "merge" ".*[Mm]erge.*"

# 4. String benchmarks
run_benchmark_suite "string" ".*String.*"

# 5. Comparison benchmarks (vs other formats)
run_benchmark_suite "comparison" ".*Comparison.*"

# 6. Complete benchmark run (all benchmarks)
echo "🔄 Running complete benchmark suite..."
complete_output="$RESULTS_DIR/complete-benchmarks-$TIMESTAMP.json"
./gradlew jmh \
    -Pjmh.resultsFile="$complete_output" \
    --console=plain

if [ -f "$complete_output" ]; then
    echo "✅ Complete benchmark suite completed: $complete_output"
    
    # Generate overall summary
    overall_summary="$RESULTS_DIR/overall-summary-$TIMESTAMP.txt"
    {
        echo "Complete Imprint Java Benchmark Results - $TIMESTAMP"
        echo "=================================================="
        echo ""
        echo "Total Benchmarks Run: $(jq '[.[] | select(.benchmark)] | length' "$complete_output" 2>/dev/null || echo "Unknown")"
        echo ""
        echo "Performance Overview:"
        echo "--------------------"
        jq -r '.[] | select(.benchmark) | "\(.benchmark): \(.primaryMetric.score | tonumber | . * 1000000 | floor / 1000000) \(.primaryMetric.scoreUnit)"' "$complete_output" 2>/dev/null | sort -k2 -n || echo "Could not generate overview"
        echo ""
    } > "$overall_summary" 2>/dev/null
    
    echo "📊 Overall summary: $overall_summary"
else
    echo "❌ Complete benchmark suite failed"
fi

echo ""
echo "🎉 Benchmark execution completed!"
echo "📁 All results saved in: $RESULTS_DIR"
echo "📄 Files created:"
ls -la "$RESULTS_DIR"/*"$TIMESTAMP"* 2>/dev/null || echo "   No files with timestamp $TIMESTAMP found"

echo ""
echo "💡 To view results:"
echo "   - JSON files can be analyzed with jq or imported into analysis tools"
echo "   - Summary files provide human-readable overviews"
echo "   - System info file contains environment details for reproducibility"