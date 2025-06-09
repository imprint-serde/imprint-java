package com.imprint.benchmark;

import org.openjdk.jmh.infra.Blackhole;

public interface Competitor {
    String name();
    void setup();
    void serialize(Blackhole bh);
    void deserialize(Blackhole bh);
    void projectAndSerialize(Blackhole bh);
    void mergeAndSerialize(Blackhole bh);
} 