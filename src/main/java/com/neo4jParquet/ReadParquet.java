package com.neo4jParquet;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;

import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.procedure.*;
import org.neo4j.procedure.builtin.BuiltInDbmsProcedures.StringResult;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;


import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import mil.nga.sf.Geometry;
import mil.nga.sf.LineString;
import mil.nga.sf.MultiPolygon;
import mil.nga.sf.Point;
import mil.nga.sf.Polygon;
import mil.nga.sf.util.GeometryUtils;
import mil.nga.sf.wkb.GeometryReader;



public class ReadParquet {
    
    @Context
    public GraphDatabaseService db;

    @Context
    public Transaction tx;

    private static final Configuration conf = new Configuration();

    @Procedure(name = "com.neo4jparquet.readParquetWKT", mode = Mode.READ)
    @Description("com.neo4jparquet.readParquetWKT(filename) - Provides the distance in grid cells between the two indexes.")
        public Stream<MapResult> readParquetWKT(
            @Name("file") String fileName)
             throws InterruptedException 
               {    
                
                String resultMessage = "";
                //System.out.println(fileName);
                ArrayList<Map<String, Object>> results = new ArrayList<>();
		
                Path file = null;
                ParquetReader<GenericRecord> reader = null;
                List<GenericRecord> allRecords = new ArrayList<GenericRecord>();
                Schema schema = null;

                try {
                    file = new Path(fileName);
                    reader = AvroParquetReader.<GenericRecord>builder(file).build();
                    
                } catch (Exception e) {
                    resultMessage = "File not found";
                }

                if (file != null && reader !=null){
                    GenericRecord record;
         
                    try {
                        while((record = reader.read()) != null) {
                           allRecords.add(record);
                              if(schema == null) {
                                 schema = record.getSchema();
                                 //System.out.println(schema);
                              }
                        }
                        reader.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        resultMessage = "IO Error";
                    }

                    try {
                        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, file, ParquetMetadataConverter.NO_FILTER);
                        MessageType schema2 = readFooter.getFileMetaData().getSchema();
                        ParquetFileReader r = new ParquetFileReader(conf, file, readFooter);
                        
                        PageReadStore pages = null;
                        while (null != (pages = r.readNextRowGroup())) {
                            final long rows = pages.getRowCount();
                            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema2);
                            final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema2));
                            for (int i = 0; i < rows; i++) {
                                final Group g = (Group) recordReader.read();
                                int fieldCount = g.getType().getFieldCount();
                                Map<String, Object> result = new HashMap<>();
				                results.add(result);                
                                for (int field = 0; field< fieldCount; field++) {
                                    int valueCount = g.getFieldRepetitionCount(field);
                                    Type fieldType = g.getType().getType(field);
                                    String fieldName = fieldType.getName();
                                    for (int index = 0; index < valueCount; index++){
                                        if (fieldType.isPrimitive()) {
                                            if (!fieldName.equals("geometry")){
                                                result.put(fieldName, g.getValueToString(field, index));
                                            } else {
                                                Geometry geometry = GeometryReader.readGeometry(g.getBinary(field, index).getBytes());
                                                String geoType = geometry.getGeometryType().toString();
                                                if (geoType.compareToIgnoreCase("POINT")==0){
                                                    mil.nga.sf.Point sfPoint  = (Point) GeometryReader.readGeometry(g.getBinary(field, index).getBytes());
                                                    String geoResults = "POINT(" + sfPoint.getY() + " " + sfPoint.getX() + ")";
                                                    result.put("Geometry",geoResults);
                                                    //System.out.println("Point info: " + sfPoint.getY() + ' ' + sfPoint.getX());
                                                }
                                                if (geoType.compareToIgnoreCase("POLYGON")==0){
                                                    int PolyCounter = 0;
                                                    mil.nga.sf.Polygon wkbPolygon  = (Polygon) GeometryReader.readGeometry(g.getBinary(field, index).getBytes());
                                                    mil.nga.sf.LineString ring = new mil.nga.sf.LineString(false, false);
                                                    String geoResults = "POLYGON((";
                                                    ring = wkbPolygon.getRing(0);
                                                    List<Point> lp = ring.getPoints();
                                                    Iterator<Point> lpIterator = lp.iterator();
                                                    Double lonCoord = 0.0;
                                                    Double latCoord = 0.0;
                                                    while (lpIterator.hasNext()) {
                                                        Point myP = lpIterator.next();
                                                        lonCoord = myP.getX();
                                                        latCoord = myP.getY();
                                                        if (PolyCounter < 1){
                                                            geoResults = geoResults + lonCoord + " " + latCoord;
                                                        } else {
                                                            geoResults = geoResults + "," + lonCoord + " " + latCoord;
                                                        }
                                                        PolyCounter++;
                                                    }
                                                    geoResults = geoResults + "))";
                                                
                                                    result.put("Geometry",  geoResults);
                                                }

                                                if (geoType.compareToIgnoreCase("LINESTRING")==0){
                                                    Double lonCoord = 0.0;
                                                    Double latCoord = 0.0;
                                                    int counter = 1;
                                                    String geoResults = "LINESTRING(";
                                                    mil.nga.sf.LineString lineString  = (LineString) GeometryReader.readGeometry(g.getBinary(field, index).getBytes());
                                                    for (Point point : lineString.getPoints()) {
                                                        lonCoord = point.getY();
                                                        latCoord = point.getX(); 
                                                        if (counter > 1){
                                                             geoResults = geoResults + ", ( " + latCoord + " " + lonCoord + ")";
                                                        } else {
                                                            geoResults = geoResults + "( " + latCoord + " " + lonCoord + ")";
                                                        }
                                                    }
                                                    geoResults = geoResults + ")";
                                                    result.put("Geometry",  geoResults);
                                                }
                                                // Multipolygon
                                                if (geoType.compareToIgnoreCase("MULTIPOLYGON")==0){
                                                    Double lonCoord = 0.0;
                                                    Double latCoord = 0.0;
                                                    String hashKey = "";
                                                    int geomCounter = 0;
                                                    String geomHashKey = "";
                                                    String geoResults = "MULTIPOLYGON((";
                                                    int PolyCounter = 0;
                                                    mil.nga.sf.MultiPolygon wkbMultiPolygon  = (MultiPolygon) GeometryReader.readGeometry(g.getBinary(field, index).getBytes());
                                                    for (Polygon polygon : wkbMultiPolygon.getPolygons()) {
                                                        Map<String, Object> multiPolyHashMap = new HashMap<>();
                                                        for (int zz=0; zz<polygon.numRings(); zz++){
                                                            mil.nga.sf.LineString ring = new mil.nga.sf.LineString(false, false);
                                                            ring = polygon.getRing(zz);
                                                            List<Point> lp = ring.getPoints();
                                                            Iterator<Point> lpIterator = lp.iterator();
                                                            List<String> strlatLongList = new ArrayList<String>();
                                                            if (PolyCounter < 1) {
                                                                geoResults = geoResults + "(";
                                                            } else {
                                                                geoResults = geoResults + ", (";
                                                            }
                                                            PolyCounter++;
                                                            int iterCounter = 0;
                                                            while (lpIterator.hasNext()) {
                                                                Point myP = lpIterator.next();
                                                                lonCoord = myP.getY();
                                                                latCoord = myP.getX();
                                                                if (iterCounter < 1){
                                                                    geoResults = geoResults +  lonCoord + " " + latCoord;
                                                                } else {
                                                                    geoResults = geoResults  + ", " + lonCoord + " " + latCoord;
                                                                }
                                                                iterCounter++;
                                                                //strlatLongList.add(latCoord + "," + lonCoord);
                                                            } 
                                                            geoResults = geoResults + ")";
                                                            //hashKey = "Ring_" + zz;

                                                            //multiPolyHashMap.put(hashKey,strlatLongList);
                                                        }
                                                        geomHashKey = "Geometry_" + geomCounter;
                                                        //result.put(geomHashKey,multiPolyHashMap);
                                                        geomCounter++;
                                                    } 
                                                    geoResults = geoResults + "))";   
                                                     result.put("Geometry",geoResults);                               
                                                }
                                            }
                                        }
                                    }
                                }

                            }
                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        resultMessage = "Error Processing Parquet File";
                    }
                }
                if (resultMessage.isEmpty()){
                    resultMessage = "Finished";
                }
                //return Stream.of(new StringResult(resultMessage));
                return results.stream().map(MapResult::new);
               }

@Procedure(name = "com.neo4jparquet.readParquet", mode = Mode.READ)
    @Description("com.neo4jparquet.readParquet(filename) - Provides the distance in grid cells between the two indexes.")
        public Stream<MapResult> readParquet(
            @Name("file") String fileName)
             throws InterruptedException 
               {    
                
                String resultMessage = "";
                //System.out.println(fileName);
                ArrayList<Map<String, Object>> results = new ArrayList<>();
		
                Path file = null;
                ParquetReader<GenericRecord> reader = null;
                List<GenericRecord> allRecords = new ArrayList<GenericRecord>();
                Schema schema = null;

                try {
                    file = new Path(fileName);
                    reader = AvroParquetReader.<GenericRecord>builder(file).build();
                    
                } catch (Exception e) {
                    resultMessage = "File not found";
                }

                if (file != null && reader !=null){
                    GenericRecord record;
         
                    try {
                        while((record = reader.read()) != null) {
                           allRecords.add(record);
                              if(schema == null) {
                                 schema = record.getSchema();
                                 //System.out.println(schema);
                              }
                        }
                        reader.close();
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        resultMessage = "IO Error";
                    }

                    try {
                        ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, file, ParquetMetadataConverter.NO_FILTER);
                        MessageType schema2 = readFooter.getFileMetaData().getSchema();
                        ParquetFileReader r = new ParquetFileReader(conf, file, readFooter);
                        
                        PageReadStore pages = null;

                        while (null != (pages = r.readNextRowGroup())) {
                            final long rows = pages.getRowCount();
                            final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema2);
                            final RecordReader recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema2));
                            for (int i = 0; i < rows; i++) {
                                final Group g = (Group) recordReader.read();
                                int fieldCount = g.getType().getFieldCount();
                                Map<String, Object> result = new HashMap<>();
				                results.add(result);                
                                for (int field = 0; field< fieldCount; field++) {
                                    int valueCount = g.getFieldRepetitionCount(field);
                                    Type fieldType = g.getType().getType(field);
                                    String fieldName = fieldType.getName();
                                    for (int index = 0; index < valueCount; index++){
                                        if (fieldType.isPrimitive()) {
                                            if (!fieldName.equals("geometry")){
                                                result.put(fieldName, g.getValueToString(field, index));
                                            } else {
                                                Geometry geometry = GeometryReader.readGeometry(g.getBinary(field, index).getBytes());
                                                String geoType = geometry.getGeometryType().toString();
                                                
                                                if (geoType.compareToIgnoreCase("POLYGON")==0){
                                                    mil.nga.sf.Polygon wkbPolygon  = (Polygon) GeometryReader.readGeometry(g.getBinary(field, index).getBytes());
                                                    mil.nga.sf.Point centroidPoint = GeometryUtils.getCentroid(wkbPolygon);
                                                    mil.nga.sf.LineString ring = new mil.nga.sf.LineString(false, false);
                                                    ring = wkbPolygon.getRing(0);
                                                    List<Point> lp = ring.getPoints();
                                                    Iterator<Point> lpIterator = lp.iterator();
                                                    List<String> strlatLongList = new ArrayList<String>();
                                                    Double lonCoord = 0.0;
                                                    Double latCoord = 0.0;
                                                    while (lpIterator.hasNext()) {
                                                        Point myP = lpIterator.next();
                                                        lonCoord = myP.getY();
                                                        latCoord = myP.getX();
                                                        strlatLongList.add(latCoord + "," + lonCoord);
                                                    }
                                                
                                                    result.put("Geometry",  strlatLongList);
                                                    result.put("Centroid",centroidPoint.getX() + " " + centroidPoint.getY());
                                                }
                                                
                                            }
                                        }
                                    }
                                }

                            }
                        }
                    } catch (IOException e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                        resultMessage = "Error Processing Parquet File";
                    }
                }
                if (resultMessage.isEmpty()){
                    resultMessage = "Finished";
                }
                //return Stream.of(new StringResult(resultMessage));
                return results.stream().map(MapResult::new);
               }
}
