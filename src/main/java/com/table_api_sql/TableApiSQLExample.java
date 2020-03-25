package com.table_api_sql;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/*
 * Table API and SQL on top of those tables
 */
public class TableApiSQLExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        tableEnv.connect(
                new FileSystem().path("/home/jivesh/avg")
        ).withFormat(
                new Csv().fieldDelimiter(',')
        ).withSchema(
                new Schema()
                        .field("date", DataTypes.STRING())
                        .field("month", DataTypes.STRING())
                        .field("category", DataTypes.STRING())
                        .field("product", DataTypes.STRING())
                        .field("profit", DataTypes.INT())
        ).inAppendMode().createTemporaryTable("CatalogTable");

        Table catalog = tableEnv.from("CatalogTable");

        /* querying with Table API */
        Table order20 = catalog
                .filter(" category === 'Category5'")
                .groupBy("month")
                .select("month, profit.sum as sum")
                .orderBy("sum");


        DataSet<Row1> order20Set = tableEnv.toDataSet(order20, Row1.class);
        order20Set.writeAsText("/home/jivesh/table1");
//        tableEnv.toAppendStream(order20, Row.class).writeAsText("/home/jivesh/table");

        /*
         * SQL
         */
        String sql = "SELECT `month`, SUM(profit) AS sum1 FROM CatalogTable WHERE category = 'Category5'"
                + " GROUP BY `month` ORDER BY sum1";
        Table order20SQL = tableEnv.sqlQuery(sql);
        DataSet<Row1> order20SQLSet = tableEnv.toDataSet(order20SQL, Row1.class);
        order20SQLSet.writeAsText("/home/jivesh/table1");


        env.execute("State");
    }

    public static class Row1 {
        public String month;
        public Integer sum;

        public Row1() {
        }

        @Override
        public String toString() {
            return month + "," + sum;
        }
    }

}


