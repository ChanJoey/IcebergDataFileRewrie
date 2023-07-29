package org.jjyc;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class DataRewriterTest {

    SparkSession spark;
    HiveCatalog hiveCatalog;
    String hiveUri = "";
    String warehouse = "";
    String dbName = "";
    String tableName = "";
    DataRewriter dataRewriter;
    Table table;
    TableIdentifier identifier;

    @Before
    public void init(){

        Map<String, String> sparkProps = new HashMap<>();
        sparkProps.put("spark.sql.extensions","org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions");
        sparkProps.put("spark.sql.catalog.hive_prod","org.apache.iceberg.spark.SparkCatalog");
        sparkProps.put("spark.sql.catalog.hive_prod.type","hive");
        sparkProps.put("spark.sql.warehouse.dir",warehouse);
        sparkProps.put("spark.sql.catalog.hive_prod.uri",hiveUri);
        sparkProps.put("iceberg.engine.hive.enabled","true");
        spark = Utils.getSparkSession("dataRewriterTest","local[1]",sparkProps);
        hiveCatalog = new HiveCatalog();
        hiveCatalog.setConf(spark.sparkContext().hadoopConfiguration());

        Map<String, String> catalogProps = new HashMap<>();
        catalogProps.put("warehouse",warehouse);
        catalogProps.put("uri",hiveUri);

        hiveCatalog.initialize("",catalogProps);

        identifier = TableIdentifier.of(dbName,tableName);
        table = hiveCatalog.loadTable(identifier);

    }

    @After
    public void close(){
        if (spark != null) {
            spark.close();
        }
    }

    @Test
    public void testDataRewriteStrategyBinPack(){
        dataRewriter = new DataRewriter.DataRewriterBuilder(spark,null)
                .setTable(table)
                .build();
        dataRewriter.doRewriteDataFiles();
    }

    @Test
    public void testDataRewriteStrategySort(){
        dataRewriter = new DataRewriter.DataRewriterBuilder(spark,null)
                .setTable(table)
                .setStrategy("sort")
                .setSortOrderColumns("i,name")
                .build();
        dataRewriter.doRewriteDataFiles();
    }

    @Test
    public void testDataRewriteStrategyzOrder(){
        dataRewriter = new DataRewriter.DataRewriterBuilder(spark,null)
                .setTable(table)
                .setStrategy("zOrder")
                .setSortOrderColumns("i,name")
                .build();
        dataRewriter.doRewriteDataFiles();
    }

    @Test
    public void testRollBack(){
        dataRewriter = new DataRewriter.DataRewriterBuilder(spark,null)
                .setTable(table)
                .build();
        dataRewriter.RollBackToLastSnapshot();
    }
}
