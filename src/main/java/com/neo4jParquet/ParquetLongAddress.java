package com.neo4jParquet;

public class ParquetLongAddress {
    public final long value;

    private ParquetLongAddress(long hex) {
        this.value = hex;
    }

    public final static ParquetLongAddress of(long value) {
        return new ParquetLongAddress(value);
    }

    public final static ParquetLongAddress of(String value) {
        if (value.toLowerCase().startsWith("0x")) {
            return new ParquetLongAddress(
                    Long.parseLong(value.substring(2), 16));
        }
        return new ParquetLongAddress(Long.parseLong(value, 16));
    }
}
