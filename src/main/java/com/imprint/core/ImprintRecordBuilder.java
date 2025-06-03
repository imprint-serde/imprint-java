package com.imprint.core;

import com.imprint.error.ImprintException;
import com.imprint.types.MapKey;
import com.imprint.types.Value;

import java.util.*;

/**
 * A fluent builder for creating ImprintRecord instances with type-safe, 
 * developer-friendly API that eliminates boilerplate Value.fromX() calls.
 * <p>
 * Field IDs can be overwritten - calling field() with the same ID multiple times
 * will replace the previous value. This allows for flexible builder patterns and
 * conditional field updates.
 * <p>
 * Usage:
 * <pre>
 *   var record = ImprintRecord.builder(schemaId)
 *       .field(1, 42)              // int to Int32Value  
 *       .field(2, "hello")         // String to StringValue
 *       .field(1, 100)             // overwrites field 1 with new value
 *       .field(3, 3.14)            // double to Float64Value
 *       .field(4, bytes)           // byte[] to BytesValue
 *       .field(5, true)            // boolean to BoolValue
 *       .nullField(6)              // to NullValue
 *       .build();
 * </pre>
 */
@SuppressWarnings("unused")
public final class ImprintRecordBuilder {
    private final SchemaId schemaId;
    private final Map<Integer, Value> fields = new TreeMap<>();
    
    ImprintRecordBuilder(SchemaId schemaId) {
        this.schemaId = Objects.requireNonNull(schemaId, "SchemaId cannot be null");
    }
    
    // Primitive types with automatic Value wrapping
    public ImprintRecordBuilder field(int id, boolean value) {
        return addField(id, Value.fromBoolean(value));
    }
    
    public ImprintRecordBuilder field(int id, int value) {
        return addField(id, Value.fromInt32(value));
    }
    
    public ImprintRecordBuilder field(int id, long value) {
        return addField(id, Value.fromInt64(value));
    }
    
    public ImprintRecordBuilder field(int id, float value) {
        return addField(id, Value.fromFloat32(value));
    }
    
    public ImprintRecordBuilder field(int id, double value) {
        return addField(id, Value.fromFloat64(value));
    }
    
    public ImprintRecordBuilder field(int id, String value) {
        return addField(id, Value.fromString(value));
    }
    
    public ImprintRecordBuilder field(int id, byte[] value) {
        return addField(id, Value.fromBytes(value));
    }
    
    // Collections with automatic conversion
    public ImprintRecordBuilder field(int id, List<? extends Object> values) {
        var convertedValues = new ArrayList<Value>(values.size());
        for (var item : values) {
            convertedValues.add(convertToValue(item));
        }
        return addField(id, Value.fromArray(convertedValues));
    }
    
    public ImprintRecordBuilder field(int id, Map<? extends Object, ? extends Object> map) {
        var convertedMap = new HashMap<MapKey, Value>(map.size());
        for (var entry : map.entrySet()) {
            var key = convertToMapKey(entry.getKey());
            var value = convertToValue(entry.getValue());
            convertedMap.put(key, value);
        }
        return addField(id, Value.fromMap(convertedMap));
    }
    
    // Nested records
    public ImprintRecordBuilder field(int id, ImprintRecord nestedRecord) {
        return addField(id, Value.fromRow(nestedRecord));
    }
    
    // Explicit null field
    public ImprintRecordBuilder nullField(int id) {
        return addField(id, Value.nullValue());
    }
    
    // Direct Value API (escape hatch for advanced usage)
    public ImprintRecordBuilder field(int id, Value value) {
        return addField(id, value);
    }
    
    // Conditional field addition
    public ImprintRecordBuilder fieldIf(boolean condition, int id, Object value) {
        if (condition) {
            return field(id, convertToValue(value));
        }
        return this;
    }
    
    public ImprintRecordBuilder fieldIfNotNull(int id, Object value) {
        return fieldIf(value != null, id, value);
    }
    
    // Bulk operations
    public ImprintRecordBuilder fields(Map<Integer, ?> fieldsMap) {
        for (var entry : fieldsMap.entrySet()) {
            field(entry.getKey(), convertToValue(entry.getValue()));
        }
        return this;
    }
    
    // Builder utilities
    public boolean hasField(int id) {
        return fields.containsKey(id);
    }
    
    public int fieldCount() {
        return fields.size();
    }
    
    public Set<Integer> fieldIds() {
        return new TreeSet<>(fields.keySet());
    }
    
    // Build the final record
    public ImprintRecord build() throws ImprintException {
        if (fields.isEmpty()) {
            throw new ImprintException(com.imprint.error.ErrorType.SCHEMA_ERROR, 
                "Cannot build empty record - add at least one field");
        }
        
        var writer = new ImprintWriter(schemaId);
        for (var entry : fields.entrySet()) {
            writer.addField(entry.getKey(), entry.getValue());
        }
        return writer.build();
    }
    
    // Internal helper methods
    /**
     * Adds or overwrites a field in the record being built.
     * If a field with the given ID already exists, it will be replaced.
     * 
     * @param id the field ID
     * @param value the field value (cannot be null - use nullField() for explicit nulls)
     * @return this builder for method chaining
     */
    private ImprintRecordBuilder addField(int id, Value value) {
        Objects.requireNonNull(value, "Value cannot be null - use nullField() for explicit null values");
        fields.put(id, value); // TreeMap.put() overwrites existing values
        return this;
    }
    
    private Value convertToValue(Object obj) {
        if (obj == null) {
            return Value.nullValue();
        }
        
        if (obj instanceof Value) {
            return (Value) obj;
        }
        
        // Auto-boxing conversion
        if (obj instanceof Boolean) {
            return Value.fromBoolean((Boolean) obj);
        }
        if (obj instanceof Integer) {
            return Value.fromInt32((Integer) obj);
        }
        if (obj instanceof Long) {
            return Value.fromInt64((Long) obj);
        }
        if (obj instanceof Float) {
            return Value.fromFloat32((Float) obj);
        }
        if (obj instanceof Double) {
            return Value.fromFloat64((Double) obj);
        }
        if (obj instanceof String) {
            return Value.fromString((String) obj);
        }
        if (obj instanceof byte[]) {
            return Value.fromBytes((byte[]) obj);
        }
        if (obj instanceof List) {
            //test
            @SuppressWarnings("unchecked")
            List<Object> list = (List<Object>) obj;
            var convertedValues = new ArrayList<Value>(list.size());
            for (var item : list) {
                convertedValues.add(convertToValue(item));
            }
            return Value.fromArray(convertedValues);
        }
        if (obj instanceof Map) {
            @SuppressWarnings("unchecked")
            Map<Object, Object> map = (Map<Object, Object>) obj;
            var convertedMap = new HashMap<MapKey, Value>(map.size());
            for (var entry : map.entrySet()) {
                var key = convertToMapKey(entry.getKey());
                var value = convertToValue(entry.getValue());
                convertedMap.put(key, value);
            }
            return Value.fromMap(convertedMap);
        }
        if (obj instanceof ImprintRecord) {
            return Value.fromRow((ImprintRecord) obj);
        }
        
        throw new IllegalArgumentException("Cannot convert " + obj.getClass().getSimpleName() + 
            " to Imprint Value. Supported types: boolean, int, long, float, double, String, byte[], List, Map, ImprintRecord");
    }
    
    private MapKey convertToMapKey(Object obj) {
        if (obj instanceof Integer) {
            return MapKey.fromInt32((Integer) obj);
        }
        if (obj instanceof Long) {
            return MapKey.fromInt64((Long) obj);
        }
        if (obj instanceof String) {
            return MapKey.fromString((String) obj);
        }
        if (obj instanceof byte[]) {
            return MapKey.fromBytes((byte[]) obj);
        }
        
        throw new IllegalArgumentException("Invalid map key type: " + obj.getClass().getSimpleName() + 
            ". Map keys must be int, long, String, or byte[]");
    }
    
    @Override
    public String toString() {
        return String.format("ImprintRecordBuilder{schemaId=%s, fields=%d}", schemaId, fields.size());
    }
}