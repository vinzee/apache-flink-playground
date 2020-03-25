package com.table_sql_api;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;

public class SqlApiExample {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment tableEnv = BatchTableEnvironment.create(env);

        /* create table from csv */
        TableSource tableSrc = CsvTableSource.builder()
                .path("/home/jivesh/avg")
                .fieldDelimiter(",")
                .field("date", Types.STRING)
                .field("month", Types.STRING)
                .field("category", Types.STRING)
                .field("product", Types.STRING)
                .field("profit", Types.INT)
                .build();

        tableEnv.registerTableSource("CatalogTable", tableSrc);

        String sql = "SELECT `month`, SUM(profit) AS sum1 FROM CatalogTable WHERE category = 'Category5'"
                + " GROUP BY `month` ORDER BY sum1";
        Table order20 = tableEnv.sqlQuery(sql);

        DataSet<Row1> order20Set = tableEnv.toDataSet(order20, Row1.class);

        order20Set.writeAsText("/home/jivesh/table_sql");
        env.execute("SQL API Example");
    }

    public static class Row1 {
        public String month;
        public Integer sum1;

        public Row1() {
        }

        @Override
        public String toString() {
            return month + "," + sum1;
        }
    }

}