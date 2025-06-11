package com.imprint.stream;

import com.imprint.core.ImprintRecord;
import com.imprint.core.SchemaId;
import com.imprint.stream.ImprintStream;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ImprintStreamTest {

    @Test
    void shouldProjectAndMergeCorrectly() throws Exception {
        // --- Setup ---
        var schemaId1 = new SchemaId(1, 1);
        var schemaId2 = new SchemaId(2, 2);
        var schemaId3 = new SchemaId(3, 3);

        var recordA = ImprintRecord.builder(schemaId1)
                .field(1, "A1")
                .field(2, 100)
                .field(3, true)
                .build();

        var recordB = ImprintRecord.builder(schemaId2)
                .field(2, 200) // Overlaps with A, should be ignored
                .field(4, "B4")
                .build();

        var recordC = ImprintRecord.builder(schemaId3)
                .field(5, 3.14)
                .field(1, "C1") // Overlaps with A, should be ignored
                .build();

        // --- Execution ---
        // Chain of operations
        var finalRecord = ImprintStream.of(recordA)
                .project(1, 3)     // Keep {1, 3} from A. Current state: {1:A, 3:A}
                .mergeWith(recordB)    // Merge B. {2:B, 4:B} are added. Current state: {1:A, 3:A, 2:B, 4:B}
                .mergeWith(recordC)    // Merge C. {5:C} is added. {1:C} is ignored. Final state: {1:A, 3:A, 2:B, 4:B, 5:C}
                .project(1, 4, 5)  // Final projection. Final result: {1:A, 4:B, 5:C}
                .toRecord();

        // --- Assertions ---
        assertNotNull(finalRecord);

        // Check final field count.
        assertEquals(3, finalRecord.getDirectory().size());

        // Check that the correct fields are present and have the right values
        assertTrue(finalRecord.hasField(1));
        assertEquals("A1", finalRecord.getString(1)); // From recordA

        assertTrue(finalRecord.hasField(4));
        assertEquals("B4", finalRecord.getString(4)); // From recordB

        assertTrue(finalRecord.hasField(5));
        assertEquals(3.14, finalRecord.getFloat64(5), 0.001); // From recordC

        // Check that dropped/ignored fields are not present
        assertFalse(finalRecord.hasField(2));
        assertFalse(finalRecord.hasField(3));
    }

    @Test
    void shouldProjectAfterMerge() throws Exception {
        var recordA = ImprintRecord.builder(new SchemaId(1, 1)).field(1, "A").field(2, 100).build();
        var recordB = ImprintRecord.builder(new SchemaId(1, 1)).field(2, 200).field(3, "B").build();

        var finalRecord = ImprintStream.of(recordA)
                .mergeWith(recordB) // virtual record is {1:A, 2:A, 3:B}
                .project(1, 3)      // final record is {1:A, 3:B}
                .toRecord();

        assertEquals(2, finalRecord.getDirectory().size());
        assertTrue(finalRecord.hasField(1));
        assertEquals("A", finalRecord.getString(1));
        assertTrue(finalRecord.hasField(3));
        assertEquals("B", finalRecord.getString(3));
        assertFalse(finalRecord.hasField(2));
    }
}