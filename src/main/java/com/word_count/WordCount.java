package com.word_count;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;

public class WordCount {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);
        String workingDirectory = System.getProperty("user.dir");

        DataSet<String> text = env.readTextFile(workingDirectory + "/src/main/java/com/word_count/words.txt");
        DataSet<String> filtered = text.filter((FilterFunction<String>) value -> value.startsWith("N"));
        DataSet<Tuple2<String, Integer>> tokenized = filtered.map(new Tokenizer());
        DataSet<Tuple2<String, Integer>> counts = tokenized.groupBy(new int[]{0}).sum(1);

        counts.print();
        System.out.println("Finish print");

        counts.writeAsCsv(workingDirectory + "/src/main/java/com/word_count/output.csv", "\n", " ", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        System.out.println("Start execution");
        env.execute("WordCount Example");
        System.out.println("Yay, its done!");
    }

    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2<>(value, 1);
        }
    }
}
