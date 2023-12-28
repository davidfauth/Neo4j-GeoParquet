package com.neo4jParquet;

import java.util.Collections;
import java.util.Map;

public class MapResult {
    private static final com.neo4jParquet.MapResult EMPTY = new com.neo4jParquet.MapResult(Collections.emptyMap());
    public final Map<String, Object> value;

    public static com.neo4jParquet.MapResult empty() {
        return EMPTY;
    }
    public  boolean isEmpty() { return value.isEmpty(); }

    public MapResult(Map<String, Object> value) {
        this.value = value;
    }
}