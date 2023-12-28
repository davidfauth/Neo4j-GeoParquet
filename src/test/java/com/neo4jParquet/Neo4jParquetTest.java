package com.neo4jParquet;

import org.assertj.core.api.Assertions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.driver.Driver;
import org.neo4j.driver.GraphDatabase;
import org.neo4j.driver.Result;
import org.neo4j.driver.Session;
import org.neo4j.harness.Neo4j;
import org.neo4j.harness.Neo4jBuilders;

import com.neo4jParquet.ReadParquet;

import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;

public class Neo4jParquetTest {
    private static Driver driver;
    private static Neo4j embeddedDatabaseServer;

    @Test
   public void run_parquet_tests() throws InterruptedException {
        embeddedDatabaseServer = Neo4jBuilders.newInProcessBuilder()
            .withDisabledServer()
            .withProcedure(ReadParquet.class)
            .withFunction(ReadParquet.class)
            .build();
        driver = GraphDatabase.driver(embeddedDatabaseServer.boltURI());
        try (Session session = driver.session()) {
            Result result = null;
            String filePath = "src/main/resources/Weather.parquet";
            result=session.run("call com.neo4jparquet.readParquet(\"src/main/resources/Weather.parquet\") yield value return value.RainTomorrow as rt limit 1");
            assertEquals("\"Yes\"", result.single().get("rt").toString());

            result=session.run("call com.neo4jparquet.readParquet(\"src/main/resources/example.parquet\") yield value with value where value.Centroid is not null return value.Centroid as centroid limit 1");
            assertEquals("\"34.75298985475595 -6.257732428506092\"", result.single().get("centroid").toString());

        }
   }

    
}
