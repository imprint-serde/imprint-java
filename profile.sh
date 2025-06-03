#!/bin/bash

# Profiling helper script for Imprint Java implementation
# 
# Usage:
#   ./profile.sh [test_method] [profiler]
#
# test_method: profileFieldAccess, profileSerialization, profileProjection, profileMemoryAllocation
# profiler: asyncprofiler, jfr, visualvm
#
# Examples:
#   ./profile.sh profileFieldAccess asyncprofiler
#   ./profile.sh profileSerialization jfr
#   ./profile.sh profileMemoryAllocation

set -e

TEST_METHOD=${1:-profileFieldAccess}
PROFILER=${2:-asyncprofiler}

echo "🔬 Starting profiling session for $TEST_METHOD using $PROFILER"

# Enable the profiler test by removing @Disabled
sed -i 's/@Disabled.*/@Test/' src/test/java/com/imprint/benchmark/ProfilerTest.java

case $PROFILER in
    "asyncprofiler")
        echo "📊 Using async-profiler (download from https://github.com/jvm-profiling-tools/async-profiler)"
        echo "   Will generate CPU profile in profiler-results/"
        mkdir -p profiler-results
        
        # Run test in background and profile it
        ./gradlew test --tests "*ProfilerTest.$TEST_METHOD" \
            -Dorg.gradle.jvmargs="-XX:+UnlockDiagnosticVMOptions -XX:+DebugNonSafepoints" &
        
        TEST_PID=$!
        sleep 2
        
        # Find the actual Java process (Gradle daemon)
        JAVA_PID=$(pgrep -f "ProfilerTest.$TEST_METHOD" | head -1)
        
        if [ -n "$JAVA_PID" ]; then
            echo "   Profiling Java process $JAVA_PID"
            if command -v async-profiler.jar >/dev/null 2>&1; then
                java -jar async-profiler.jar -d 30 -f profiler-results/profile-$TEST_METHOD.html $JAVA_PID
            else
                echo "   ⚠️  async-profiler.jar not found in PATH"
                echo "   📥 Download from: https://github.com/jvm-profiling-tools/async-profiler/releases"
            fi
        fi
        
        wait $TEST_PID
        ;;
        
    "jfr")
        echo "📊 Using Java Flight Recorder"
        mkdir -p profiler-results
        
        ./gradlew test --tests "*ProfilerTest.$TEST_METHOD" \
            -Dorg.gradle.jvmargs="-XX:+FlightRecorder -XX:StartFlightRecording=duration=60s,filename=profiler-results/profile-$TEST_METHOD.jfr,settings=profile"
        
        echo "   📂 JFR file saved to: profiler-results/profile-$TEST_METHOD.jfr"
        echo "   🔍 Open with: jmc profiler-results/profile-$TEST_METHOD.jfr"
        ;;
        
    "visualvm")
        echo "📊 Using VisualVM"
        echo "   1. Start VisualVM: jvisualvm"
        echo "   2. Enable the ProfilerTest manually"
        echo "   3. Run: ./gradlew test --tests '*ProfilerTest.$TEST_METHOD' --debug-jvm"
        echo "   4. Attach VisualVM to the Gradle daemon process"
        echo "   5. Start CPU/Memory profiling"
        
        read -p "Press Enter when VisualVM is ready..."
        ./gradlew test --tests "*ProfilerTest.$TEST_METHOD" --debug-jvm
        ;;
        
    *)
        echo "❌ Unknown profiler: $PROFILER"
        echo "   Supported: asyncprofiler, jfr, visualvm"
        exit 1
        ;;
esac

# Restore @Disabled annotation
sed -i 's/@Test/@Disabled("Enable manually for profiling")/' src/test/java/com/imprint/benchmark/ProfilerTest.java

echo "✅ Profiling complete!"
echo ""
echo "🔍 Key areas to examine:"
echo "   • Object allocation hotspots (new, arrays, strings)"
echo "   • ByteBuffer operations and slicing"
echo "   • String UTF-8 encoding/decoding"
echo "   • Binary search in directory lookup"
echo "   • TypeHandler method dispatch"
echo "   • VarInt encoding/decoding"
echo ""
echo "📊 Profile results in: profiler-results/"