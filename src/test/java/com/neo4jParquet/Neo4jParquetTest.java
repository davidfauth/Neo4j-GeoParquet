package com.neo4jParquet;

import org.assertj.core.api.Assertions;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

            result=session.run("call com.neo4jparquet.readParquetWKT(\"src/main/resources/examples.parquet\") yield value return value.errorMessage as errorMessage limit 1");
            assertEquals("\"-1\"", result.single().get("errorMessage").toString());

            result=session.run("call com.neo4jparquet.readParquetWKT(\"src/main/resources/multilinestring.parquet\") yield value return value.Geometry as Geometry limit 1");
            assertEquals("\"MULTILINESTRING(( 100.0 0.0), ( 101.0 1.0), ( 102.0 2.0), ( 103.0 3.0))\"", result.single().get("Geometry").toString());

        }
   }

    
}
