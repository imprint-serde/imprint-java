package com.imprint;

import com.imprint.core.*;
import com.imprint.types.*;
import com.imprint.error.ImprintException;
import java.util.*;

/**
 * Integration test to verify the complete Java implementation works.
 * This can be run as a simple main method without JUnit.
 */
public class IntegrationTest {
    
    public static void main(String[] args) {
        try {
            testBasicFunctionality();
            testArraysAndMaps();
            testNestedRecords();
            System.out.println("All integration tests passed!");
        } catch (Exception e) {
            System.err.println("Integration test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }
    
    static void testBasicFunctionality() throws ImprintException {
        System.out.println("Testing basic functionality...");
        
        SchemaId schemaId = new SchemaId(1, 0xdeadbeef);
        ImprintWriter writer = new ImprintWriter(schemaId);
        
        writer.addField(1, Value.fromInt32(42))
              .addField(2, Value.fromString("testing java imprint spec"))
              .addField(3, Value.fromBoolean(true))
              .addField(4, Value.fromFloat64(3.14159))
              .addField(5, Value.fromBytes(new byte[]{1, 2, 3, 4}));
              
        ImprintRecord record = writer.build();
        
        // Verify we can read values back
        assert record.getValue(1).get().equals(Value.fromInt32(42));
        assert record.getValue(2).get().equals(Value.fromString("testing java imprint spec"));
        assert record.getValue(3).get().equals(Value.fromBoolean(true));
        assert record.getValue(999).isEmpty(); // non-existent field
        
        // Test serialization round-trip
        var buffer = record.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        ImprintRecord deserialized = ImprintRecord.deserialize(serialized);
        
        assert deserialized.getValue(1).get().equals(Value.fromInt32(42));
        assert deserialized.getValue(2).get().equals(Value.fromString("testing java imprint spec"));
        assert deserialized.getValue(3).get().equals(Value.fromBoolean(true));
        
        System.out.println("✓ Basic functionality test passed");
    }
    
    static void testArraysAndMaps() throws ImprintException {
        System.out.println("Testing arrays and maps...");
        
        SchemaId schemaId = new SchemaId(2, 0xcafebabe);
        ImprintWriter writer = new ImprintWriter(schemaId);
        
        // Create an array
        List<Value> intArray = Arrays.asList(
            Value.fromInt32(1),
            Value.fromInt32(2),
            Value.fromInt32(3)
        );
        
        // Create a map
        Map<MapKey, Value> stringToIntMap = new HashMap<>();
        stringToIntMap.put(MapKey.fromString("one"), Value.fromInt32(1));
        stringToIntMap.put(MapKey.fromString("two"), Value.fromInt32(2));
        
        writer.addField(1, Value.fromArray(intArray))
              .addField(2, Value.fromMap(stringToIntMap));
              
        ImprintRecord record = writer.build();
        
        // Test serialization round-trip
        var buffer = record.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        ImprintRecord deserialized = ImprintRecord.deserialize(serialized);
        
        // Verify array
        Value arrayValue = deserialized.getValue(1).get();
        assert arrayValue instanceof Value.ArrayValue;
        List<Value> deserializedArray = ((Value.ArrayValue) arrayValue).getValue();
        assert deserializedArray.size() == 3;
        assert deserializedArray.get(0).equals(Value.fromInt32(1));
        
        // Verify map
        Value mapValue = deserialized.getValue(2).get();
        assert mapValue instanceof Value.MapValue;
        Map<MapKey, Value> deserializedMap = ((Value.MapValue) mapValue).getValue();
        assert deserializedMap.size() == 2;
        assert deserializedMap.get(MapKey.fromString("one")).equals(Value.fromInt32(1));
        
        System.out.println("✓ Arrays and maps test passed");
    }
    
    static void testNestedRecords() throws ImprintException {
        System.out.println("Testing nested records...");
        
        // Create inner record
        SchemaId innerSchemaId = new SchemaId(3, 0x12345678);
        ImprintWriter innerWriter = new ImprintWriter(innerSchemaId);
        innerWriter.addField(1, Value.fromString("nested data"))
                   .addField(2, Value.fromInt64(9876543210L));
        ImprintRecord innerRecord = innerWriter.build();
        
        // Create outer record
        SchemaId outerSchemaId = new SchemaId(4, 0x87654321);
        ImprintWriter outerWriter = new ImprintWriter(outerSchemaId);
        outerWriter.addField(1, Value.fromRow(innerRecord))
                   .addField(2, Value.fromString("outer data"));
        ImprintRecord outerRecord = outerWriter.build();
        
        // Test serialization round-trip
        var buffer = outerRecord.serializeToBuffer();
        byte[] serialized = new byte[buffer.remaining()];
        buffer.get(serialized);
        ImprintRecord deserialized = ImprintRecord.deserialize(serialized);
        
        // Verify outer record
        assert deserialized.getHeader().getSchemaId().getFieldspaceId() == 4;
        assert deserialized.getValue(2).get().equals(Value.fromString("outer data"));
        
        // Verify nested record
        Value rowValue = deserialized.getValue(1).get();
        assert rowValue instanceof Value.RowValue;
        ImprintRecord nestedRecord = ((Value.RowValue) rowValue).getValue();
        
        assert nestedRecord.getHeader().getSchemaId().getFieldspaceId() == 3;
        assert nestedRecord.getValue(1).get().equals(Value.fromString("nested data"));
        assert nestedRecord.getValue(2).get().equals(Value.fromInt64(9876543210L));
        
        System.out.println("✓ Nested records test passed");
    }
}