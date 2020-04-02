package com.dataset_api;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

@SuppressWarnings("serial")
public class Joins {
    public static void main(String[] args) throws Exception {
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool params = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(params);

        // Read person file and generate tuples out of each string read
        //presonSet = tuple of (1  John)
        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("input1"))
                .map((MapFunction<String, Tuple2<Integer, String>>) value -> {
                    String[] words = value.split(",");                                                 // words = [ {1} {John}]
                    return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
                });
        // Read location file and generate tuples out of each string read
        //locationSet = tuple of (1  DC)
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2")).
                map((MapFunction<String, Tuple2<Integer, String>>) value -> {
                    String[] words = value.split(",");
                    return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
                });

        // InnerJoin
        DataSet<Tuple3<Integer, String, String>> innerJoin = personSet.join(locationSet).where(0).equalTo(0)
                .with((JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>) (person, location) -> {
                    // returns tuple of (1 John DC)
                    return new Tuple3<>(person.f0, person.f1, location.f1);
                });

        innerJoin.writeAsCsv(params.get("output"), "\n", " ");

        // LeftOuterJoin
        DataSet<Tuple3<Integer, String, String>> leftOuterJoin = personSet.leftOuterJoin(locationSet).where(0).equalTo(0)
                .with((JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>) (person, location) -> {
                    if (location == null) {
                        return new Tuple3<>(person.f0, person.f1, "NULL");
                    }

                    return new Tuple3<>(person.f0, person.f1, location.f1);
                });

        leftOuterJoin.writeAsCsv(params.get("output"), "\n", " ");

        // RightOuterJoin
        DataSet<Tuple3<Integer, String, String>> rightOuterJoin = personSet.rightOuterJoin(locationSet).where(0).equalTo(0)
                .with((JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>) (person, location) -> {
                    if (person == null) {
                        return new Tuple3<>(location.f0, "NULL", location.f1);
                    }
                    return new Tuple3<>(person.f0, person.f1, location.f1);
                });//.collect();

        rightOuterJoin.writeAsCsv(params.get("output"), "\n", " ");

        // FullOuterJoin
        DataSet<Tuple3<Integer, String, String>> fullOuterJoin = personSet.fullOuterJoin(locationSet).where(0).equalTo(0)
                .with((JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>) (person, location) -> {
                    if (location == null) {
                        return new Tuple3<>(person.f0, person.f1, "NULL");
                    }
                    // for rightOuterJoin
                    else if (person == null)
                        return new Tuple3<>(location.f0, "NULL", location.f1);

                    return new Tuple3<>(person.f0, person.f1, location.f1);
                });
        fullOuterJoin.writeAsCsv(params.get("output"), "\n", " ");

        env.execute("Join example");
    }
}
