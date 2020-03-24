package p1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.sources.CsvTableSource;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;

public class TableApiExample {

	public static void main(String[] args) throws Exception
	{
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		BatchTableEnvironment tableEnv = TableEnvironment.getTableEnvironment(env);
		
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
		
		Table catalog = tableEnv.scan("CatalogTable");
		
		/* querying with Table API */
		Table order20 = catalog
				.filter(" category === 'Category5'")
				.groupBy("month")
				.select("month, profit.sum as sum")
				.orderBy("sum");
		
		
		DataSet<Row1> order20Set = tableEnv.toDataSet(order20, Row1.class);
		
		order20Set.writeAsText("/home/jivesh/table1");
		//tableEnv.toAppendStream(order20, Row.class).writeAsText("/home/jivesh/table");
		env.execute("State");
	}
	
	public static class Row1
	{
		public String month;
		public Integer sum;
		
		public Row1(){}
		
		public String toString()
		{
			return month + "," + sum;
		}	
	}
	
}


