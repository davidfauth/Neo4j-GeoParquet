package com.neo4jParquet;

public class ParquetStringAddress {
    public final String value;

    private ParquetStringAddress(String hex) {
        this.value = hex;
    }

    public final static ParquetStringAddress of(long value) {
        return new ParquetStringAddress(Long.toHexString(value));
    }

    public final static ParquetStringAddress of(String value) {
        return new ParquetStringAddress(value);
    }
}
