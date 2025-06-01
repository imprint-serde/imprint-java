package com.imprint.core;

import com.imprint.error.ErrorType;
import com.imprint.error.ImprintException;
import com.imprint.types.MapKey;
import com.imprint.types.Value;
import com.imprint.util.VarInt;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.*;

/**
 * A writer for constructing ImprintRecords by adding fields sequentially.
 */
public final class ImprintWriter {
    private final SchemaId schemaId;
    private final TreeMap<Integer, Value> fields; // keep fields in sorted order
    
    public ImprintWriter(SchemaId schemaId) {
        this.schemaId = Objects.requireNonNull(schemaId, "SchemaId cannot be null");
        this.fields = new TreeMap<>();
    }
    
    /**
     * Adds a field to the record being built.
     */
    public ImprintWriter addField(int id, Value value) {
        Objects.requireNonNull(value, "Value cannot be null");
        this.fields.put(id, value);
        return this;
    }
    
    /**
     * Consumes the writer and builds an ImprintRecord.
     */
    public ImprintRecord build() throws ImprintException {
        var directory = new ArrayList<DirectoryEntry>(fields.size());
        var payloadBuffer = ByteBuffer.allocate(estimatePayloadSize());
        payloadBuffer.order(ByteOrder.LITTLE_ENDIAN);
        
        for (var entry : fields.entrySet()) {
            int fieldId = entry.getKey();
            var value = entry.getValue();
            
            directory.add(new DirectoryEntry(fieldId, value.getTypeCode(), payloadBuffer.position()));
            serializeValue(value, payloadBuffer);
        }
        
        // Create read-only view of the payload without copying
        payloadBuffer.flip(); // limit = position, position = 0
        var payloadView = payloadBuffer.slice().asReadOnlyBuffer();
        
        var header = new Header(new Flags((byte) 0), schemaId, payloadView.remaining());
        return new ImprintRecord(header, directory, payloadView);
    }
    
    private int estimatePayloadSize() {
        // More accurate estimation to reduce allocations
        int estimatedSize = 0;
        for (var value : fields.values()) {
            estimatedSize += estimateValueSize(value);
        }
        // Add 25% buffer to reduce reallocations
        return Math.max(estimatedSize + (estimatedSize / 4), fields.size() * 16);
    }
    
    /**
     * Estimates the serialized size in bytes for a given value.
     * This method provides size estimates for payload buffer allocation,
     * supporting both array-based and ByteBuffer-based value types.
     * 
     * @param value the value to estimate size for
     * @return estimated size in bytes including type-specific overhead
     */
    private int estimateValueSize(Value value) {
        // Use TypeHandler for simple types
        switch (value.getTypeCode()) {
            case NULL:
            case BOOL:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BYTES:
            case STRING:
                return value.getTypeCode().getHandler().estimateSize(value);

            case ARRAY:
                List<Value> array = ((Value.ArrayValue) value).getValue();
                int arraySize = VarInt.encodedLength(array.size()) + 1; // length + type code
                for (Value element : array) {
                    arraySize += estimateValueSize(element);
                }
                return arraySize;

            case MAP:
                Map<MapKey, Value> map = ((Value.MapValue) value).getValue();
                int mapSize = VarInt.encodedLength(map.size()) + 2; // length + 2 type codes
                for (Map.Entry<MapKey, Value> entry : map.entrySet()) {
                    mapSize += estimateMapKeySize(entry.getKey());
                    mapSize += estimateValueSize(entry.getValue());
                }
                return mapSize;

            case ROW:
                // Estimate nested record size (rough approximation)
                return 100; // Conservative estimate

            default:
                return 32; // Default fallback
        }
    }
    
    private int estimateMapKeySize(MapKey key) {
        switch (key.getTypeCode()) {
            case INT32: return 4;
            case INT64: return 8;
            case BYTES:
                byte[] bytes = ((MapKey.BytesKey) key).getValue();
                return VarInt.encodedLength(bytes.length) + bytes.length;

            case STRING:
                var str = ((MapKey.StringKey) key).getValue();
                int utf8Length = str.getBytes(StandardCharsets.UTF_8).length;
                return VarInt.encodedLength(utf8Length) + utf8Length;

            default:
                return 16; // Default fallback
        }
    }
    
    private void serializeValue(Value value, ByteBuffer buffer) throws ImprintException {
        switch (value.getTypeCode()) {
            case NULL:
            case BOOL:
            case INT32:
            case INT64:
            case FLOAT32:
            case FLOAT64:
            case BYTES:
            case STRING:
                value.getTypeCode().getHandler().serialize(value, buffer);
                break;
                
            case ARRAY:
                serializeArray((Value.ArrayValue) value, buffer);
                break;
                
            case MAP:
                serializeMap((Value.MapValue) value, buffer);
                break;
                
            case ROW:
                Value.RowValue rowValue = (Value.RowValue) value;
                var serializedRow = rowValue.getValue().serializeToBuffer();
                buffer.put(serializedRow);
                break;
                
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, 
                    "Unknown type code: " + value.getTypeCode());
        }
    }
    
    private void serializeArray(Value.ArrayValue arrayValue, ByteBuffer buffer) throws ImprintException {
        var elements = arrayValue.getValue();
        VarInt.encode(elements.size(), buffer);
        
        if (elements.isEmpty()) return;

        // All elements must have the same type
        var elementType = elements.get(0).getTypeCode();
        buffer.put(elementType.getCode());
        for (var element : elements) {
            if (element.getTypeCode() != elementType) {
                throw new ImprintException(ErrorType.SCHEMA_ERROR, 
                    "Array elements must have same type code: " + 
                    element.getTypeCode() + " != " + elementType);
            }
            serializeValue(element, buffer);
        }
    }
    
    private void serializeMap(Value.MapValue mapValue, ByteBuffer buffer) throws ImprintException {
        var map = mapValue.getValue();
        VarInt.encode(map.size(), buffer);
        
        if (map.isEmpty()) {
            return;
        }
        
        // All keys and values must have consistent types
        var iterator = map.entrySet().iterator();
        var first = iterator.next();
        var keyType = first.getKey().getTypeCode();
        var valueType = first.getValue().getTypeCode();
        
        buffer.put(keyType.getCode());
        buffer.put(valueType.getCode());
        
        // Serialize the first entry
        serializeMapKey(first.getKey(), buffer);
        serializeValue(first.getValue(), buffer);
        
        // Serialize remaining entries
        while (iterator.hasNext()) {
            var entry = iterator.next();
            if (entry.getKey().getTypeCode() != keyType) {
                throw new ImprintException(ErrorType.SCHEMA_ERROR, 
                    "Map keys must have same type code: " +
                            entry.getKey().getTypeCode() + " != " + keyType);
            }
            if (entry.getValue().getTypeCode() != valueType) {
                throw new ImprintException(ErrorType.SCHEMA_ERROR, 
                    "Map values must have same type code: " +
                            entry.getValue().getTypeCode() + " != " + valueType);
            }
            
            serializeMapKey(entry.getKey(), buffer);
            serializeValue(entry.getValue(), buffer);
        }
    }
    
    private void serializeMapKey(MapKey key, ByteBuffer buffer) throws ImprintException {
        switch (key.getTypeCode()) {
            case INT32:
                MapKey.Int32Key int32Key = (MapKey.Int32Key) key;
                buffer.putInt(int32Key.getValue());
                break;
                
            case INT64:
                MapKey.Int64Key int64Key = (MapKey.Int64Key) key;
                buffer.putLong(int64Key.getValue());
                break;
                
            case BYTES:
                MapKey.BytesKey bytesKey = (MapKey.BytesKey) key;
                byte[] bytes = bytesKey.getValue();
                VarInt.encode(bytes.length, buffer);
                buffer.put(bytes);
                break;
                
            case STRING:
                MapKey.StringKey stringKey = (MapKey.StringKey) key;
                byte[] stringBytes = stringKey.getValue().getBytes(StandardCharsets.UTF_8);
                VarInt.encode(stringBytes.length, buffer);
                buffer.put(stringBytes);
                break;
                
            default:
                throw new ImprintException(ErrorType.SERIALIZATION_ERROR, 
                    "Invalid map key type: " + key.getTypeCode());
        }
    }
}