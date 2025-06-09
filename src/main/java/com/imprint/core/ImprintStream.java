package com.imprint.core;

import com.imprint.error.ImprintException;
import it.unimi.dsi.fastutil.ints.Int2ObjectAVLTreeMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectSortedMap;
import it.unimi.dsi.fastutil.ints.IntSet;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.*;

/**
 * Provides a framework for lazy, zero-copy transformations of Imprint records.
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
        final IntSet fieldIds;

        private ProjectPlan(Plan previous, int... fieldIds) {
            this.previous = Objects.requireNonNull(previous);
            this.fieldIds = new IntOpenHashSet();
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
            // Unwind the plan's linked-list structure into a forward-order list of operations.
            var planList = getPlans();
            Collections.reverse(planList);

            // This map holds the set of fields being built, sorted by field ID.
            var resolvedFields = new Int2ObjectAVLTreeMap<FieldSource>();

            // Iteratively evaluate the plan step-by-step.
            for (var planStep : planList) {
                if (planStep instanceof SourcePlan) {
                    var sourcePlan = (SourcePlan) planStep;
                    for (var entry : sourcePlan.source.getDirectory()) {
                        resolvedFields.put(entry.getId(), new FieldSource(sourcePlan.source, entry));
                    }
                } else if (planStep instanceof ProjectPlan) {
                    var projectPlan = (ProjectPlan) planStep;
                    // Apply projection to the current state of resolved fields.
                    // Keep only fields that are in the projection set
                    var keysToRemove = new IntOpenHashSet();
                    for (int fieldId : resolvedFields.keySet()) {
                        if (!projectPlan.fieldIds.contains(fieldId)) {
                            keysToRemove.add(fieldId);
                        }
                    }
                    for (int keyToRemove : keysToRemove) {
                        resolvedFields.remove(keyToRemove);
                    }
                } else if (planStep instanceof MergePlan) {
                    var mergePlan = (MergePlan) planStep;
                    // Add fields from other records if they aren't already in the map.
                    for (var otherRecord : mergePlan.others) {
                        for (var entry : otherRecord.getDirectory()) {
                            int fieldId = entry.getId();
                            if (!resolvedFields.containsKey(fieldId)) {
                                resolvedFields.put(fieldId, new FieldSource(otherRecord, entry));
                            }
                        }
                    }
                }
            }

            // Once the final field set is determined, build the record.
            return build(resolvedFields);
        }

        private ArrayList<Plan> getPlans() {
            var planList = new ArrayList<Plan>();
            var current = plan;
            while (current != null) {
                planList.add(current);
                if (current instanceof ProjectPlan) {
                    current = ((ProjectPlan) current).previous;
                } else if (current instanceof MergePlan) {
                    current = ((MergePlan) current).previous;
                } else if (current instanceof SourcePlan) {
                    current = null; // End of the chain
                }
            }
            return planList;
        }

        private ImprintRecord build(Int2ObjectSortedMap<FieldSource> finalFields) {
            if (finalFields.isEmpty()) {
                // To-Do: Need a way to get the schemaId for an empty record.
                // For now, returning null or using a default.
                try {
                    return ImprintRecord.builder(new SchemaId(0, 0)).build();
                } catch (ImprintException e) {
                    // This should not happen when building an empty record.
                    throw new IllegalStateException("Failed to build empty record.", e);
                }
            }

            // Determine the schema from the first field's source record.
            SchemaId schemaId = finalFields.get(finalFields.firstIntKey()).record.getHeader().getSchemaId();

            // 1. Calculate final payload size and prepare directory.
            int payloadSize = 0;
            var newDirectoryMap = new Int2ObjectAVLTreeMap<DirectoryEntry>();

            // Iterate over fields in sorted order
            for (var entry : finalFields.int2ObjectEntrySet()) {
                int fieldId = entry.getIntKey();
                var fieldSource = entry.getValue();
                int fieldLength = fieldSource.getLength();

                newDirectoryMap.put(fieldId, new SimpleDirectoryEntry(
                        fieldSource.entry.getId(),
                        fieldSource.entry.getTypeCode(),
                        payloadSize));
                payloadSize += fieldLength;
            }

            // 2. Allocate buffer and copy data.
            var payload = ByteBuffer.allocate(payloadSize).order(ByteOrder.LITTLE_ENDIAN);
            for (var fieldSource : finalFields.values()) {
                try {
                    ByteBuffer sourceData = fieldSource.record.getRawBytes(fieldSource.entry.getId());
                    if (sourceData != null) {
                        payload.put(sourceData.duplicate());
                    }
                } catch (Exception e) {
                    // This indicates a data corruption or bug, shouldn't happen in normal operation.
                    throw new IllegalStateException("Failed to copy data for field " + fieldSource.entry.getId(), e);
                }
            }
            payload.flip();

            // 3. Construct the final record.
            var newHeader = new Header(new Flags((byte) 0), schemaId, payload.remaining());
            return new ImprintRecord(newHeader, newDirectoryMap, payload.asReadOnlyBuffer());
        }

        /**
         * A helper class to track the source record and directory entry for a field.
         */
        private static final class FieldSource {
            final ImprintRecord record;
            final DirectoryEntry entry;

            FieldSource(ImprintRecord record, DirectoryEntry entry) {
                this.record = record;
                this.entry = entry;
            }

            int getLength() {
                try {
                    ByteBuffer buf = record.getRawBytes(entry.getId());
                    return buf != null ? buf.remaining() : 0;
                } catch (Exception e) {
                    return 0;
                }
            }
        }
    }
}