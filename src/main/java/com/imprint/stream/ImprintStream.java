package com.imprint.stream;

import com.imprint.core.*;
import com.imprint.error.ImprintException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * Provides a framework for lazy, (eventual) zero-copy transformations of Imprint records.
 * <p>
 * Operations like {@link #project(int...)} and {@link #mergeWith(ImprintRecord)} are
 * intermediate and do not create new records. They build up a plan of operations
 * that is executed only when a terminal operation like {@link #toRecord()} is called.
 */
public final class ImprintStream {

    private final Plan plan;

    private ImprintStream(Plan plan) {
        this.plan = Objects.requireNonNull(plan);
    }

    /**
     * The internal representation of the transformation plan.
     * This is a linked-list style structure where each step points to the previous one.
     */
    private interface Plan {
        // Marker interface for the plan steps
    }

    /**
     * The starting point of a plan, containing the initial source record.
     */
    private static final class SourcePlan implements Plan {
        final ImprintRecord source;

        private SourcePlan(ImprintRecord source) {
            this.source = Objects.requireNonNull(source, "Source record cannot be null.");
        }
    }

    /**
     * A plan step representing a 'project' operation.
     */
    private static final class ProjectPlan implements Plan {
        final Plan previous;
        final Set<Integer> fieldIds;

        private ProjectPlan(Plan previous, int... fieldIds) {
            this.previous = Objects.requireNonNull(previous);
            this.fieldIds = new HashSet<>();
            for (int id : fieldIds) {
                this.fieldIds.add(id);
            }
        }
    }

    /**
     * A plan step representing a 'merge' operation.
     */
    private static final class MergePlan implements Plan {
        final Plan previous;
        final List<ImprintRecord> others;

        private MergePlan(Plan previous, List<ImprintRecord> others) {
            this.previous = Objects.requireNonNull(previous);
            this.others = Objects.requireNonNull(others);
        }
    }

    // ========== PUBLIC API ==========

    /**
     * Creates a new transformation stream starting with a source record.
     *
     * @param source The initial record for the transformation.
     * @return A new ImprintStream.
     */
    public static ImprintStream of(ImprintRecord source) {
        return new ImprintStream(new SourcePlan(source));
    }

    /**
     * An intermediate operation that defines a projection on the stream.
     * This is a lazy operation; the projection is only performed when a terminal
     * operation is called.
     *
     * @param fieldIds The field IDs to keep in the final record.
     * @return A new ImprintStream with the projection step added to its plan.
     */
    public ImprintStream project(int... fieldIds) {
        return new ImprintStream(new ProjectPlan(this.plan, fieldIds));
    }

    /**
     * An intermediate operation that defines a merge on the stream.
     * The record from this stream (the "left" side) takes precedence in case
     * of overlapping field IDs.
     * <p>
     * This is a lazy operation; the merge is only performed when a terminal
     * operation is called.
     *
     * @param other The record to merge with this stream's record.
     * @return A new ImprintStream with the merge step added to its plan.
     */
    public ImprintStream mergeWith(ImprintRecord other) {
        return new ImprintStream(new MergePlan(this.plan, Collections.singletonList(other)));
    }

    /**
     * A terminal operation that executes the defined transformation plan and
     * constructs a new, consolidated ImprintRecord.
     *
     * @return a new ImprintRecord representing the result of the stream operations.
     */
    public ImprintRecord toRecord() {
        return new Evaluator(this.plan).execute();
    }

    // ========== EVALUATOR ==========

    /**
     * The engine that walks the plan and executes the transformation.
     */
    private static final class Evaluator {
        private final Plan plan;

        private Evaluator(Plan plan) {
            this.plan = plan;
        }

        public ImprintRecord execute() {
            // Unwind the plan from a deque
            var planQueue = getPlans();

            // Set of fields being built
            var resolvedFields = new TreeMap<Integer, FieldSource>();

            for (var planStep : planQueue) {
                if (planStep instanceof SourcePlan) {
                    var sourcePlan = (SourcePlan) planStep;
                    for (var entry : sourcePlan.source.getDirectory()) {
                        resolvedFields.put((int) entry.getId(), new FieldSource(sourcePlan.source, entry));
                    }
                } else if (planStep instanceof ProjectPlan) {
                    var projectPlan = (ProjectPlan) planStep;
                    // Apply projection to the current state of resolved fields.
                    // Keep only fields that are in the projection set
                    resolvedFields.keySet().removeIf(fieldId -> !projectPlan.fieldIds.contains(fieldId));
                } else if (planStep instanceof MergePlan) {
                    var mergePlan = (MergePlan) planStep;
                    // Add fields from other records if they aren't already in the map.
                    for (var otherRecord : mergePlan.others) {
                        for (var entry : otherRecord.getDirectory()) {
                            int fieldId = entry.getId();
                            resolvedFields.putIfAbsent(fieldId, new FieldSource(otherRecord, entry));
                        }
                    }
                }
            }
            return build(resolvedFields);
        }

        private Deque<Plan> getPlans() {
            var planQueue = new ArrayDeque<Plan>();
            var current = plan;
            while (current != null) {
                planQueue.addFirst(current);
                if (current instanceof ProjectPlan) {
                    current = ((ProjectPlan) current).previous;
                } else if (current instanceof MergePlan) {
                    current = ((MergePlan) current).previous;
                } else if (current instanceof SourcePlan) {
                    current = null; // End of the chain
                }
            }
            return planQueue;
        }

        private ImprintRecord build(NavigableMap<Integer, FieldSource> finalFields) {
            if (finalFields.isEmpty()) {
                // TODO: Need a way to get the schemaId for an empty record.
                // For now, returning null or using a default.
                try {
                    return ImprintRecord.builder(new SchemaId(0, 0)).build();
                } catch (ImprintException e) {
                    // TODO This shouldn't really ever happen, we probably need a better way of consolidating error handling
                    throw new IllegalStateException("Failed to build empty record.", e);
                }
            }

            // Use schema from the first field's source record.
            var schemaId = finalFields.firstEntry().getValue().record.getHeader().getSchemaId();

            // 1. Calculate final payload size and prepare directory.
            int payloadSize = 0;
            var newDirectoryMap = new TreeMap<Integer, Directory>();

            for (var entry : finalFields.entrySet()) {
                int fieldId = entry.getKey();
                var fieldSource = entry.getValue();
                int fieldLength = fieldSource.getLength();

                newDirectoryMap.put(fieldId, new Directory.Entry(fieldSource.entry.getId(), fieldSource.entry.getTypeCode(), payloadSize));
                payloadSize += fieldLength;
            }

            // 2. Allocate buffer and copy data.
            var payload = ByteBuffer.allocate(payloadSize).order(ByteOrder.LITTLE_ENDIAN);
            for (var fieldSource : finalFields.values()) {
                try {
                    var sourceData = fieldSource.record.getRawBytes(fieldSource.entry.getId());
                    if (sourceData != null)
                        payload.put(sourceData.duplicate());
                } catch (Exception e) {
                    // Shouldn't happen in normal operation - maybe some sort of data corruption or race issue
                    throw new IllegalStateException("Failed to copy data for field " + fieldSource.entry.getId(), e);
                }
            }
            payload.flip();

            // 3. Construct the final record.
            var newHeader = new Header(new Flags((byte) 0), schemaId, payload.remaining());
            return new ImprintRecord(newHeader, newDirectoryMap, payload.asReadOnlyBuffer());
        }

        /**
         * A lightweight struct to track the source of a field during evaluation.
         */
        private static final class FieldSource {
            final ImprintRecord record;
            final Directory entry;

            FieldSource(ImprintRecord record, Directory entry) {
                this.record = record;
                this.entry = entry;
            }

            int getLength() {
                try {
                    var buf = record.getRawBytes(entry.getId());
                    return buf != null ? buf.remaining() : 0;
                } catch (Exception e) {
                    return 0;
                }
            }
        }
    }
}