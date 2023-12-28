# Neo4j Read Parquet
This library provides a set of procedures for Neo4j 5 for reading parquet files including GeoParquet (https://geoparquet.org/).
I'm primarily interested in the GeoParquet use cases but can also use this for reading Parquet files.


Instructions
------------ 

This project uses maven, to build a jar-file with the procedure in this
project, simply package the project with maven:

    mvn clean package

This will produce a jar-file, `neo4jparquet-0.1.0.jar`,
that can be copied to the `plugins` directory of your Neo4j instance.

    cp target/neo4jparquet-0.1.0.jar  neo4j-enterprise-5.x.0/plugins/.


Edit your Neo4j/conf/neo4j.conf file by adding this line:

    dbms.security.procedures.unrestricted=apoc.*,gds.*,com.neo4jparquet.*
	dbms.security.procedures.allowlist=apoc.*,gds.*,com.neo4jparquet.*
   
    
(Re)start Neo4j

# Documentation
Refer to the Documentation.md file for detailed documenation on the procedures.


