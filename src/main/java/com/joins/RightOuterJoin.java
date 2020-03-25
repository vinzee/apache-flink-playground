package p1;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

@SuppressWarnings("serial")
public class RightOuterJoin {
    public static void main(String[] args) throws Exception {

        // set up the execution environment
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        final ParameterTool params = ParameterTool.fromArgs(args);
        // make parameters available in the web interface

        env.getConfig().setGlobalJobParameters(params);

        // 1, Name
        DataSet<Tuple2<Integer, String>> personSet = env.readTextFile(params.get("input1")).           //presonSet = tuple of (1  John)
                map((MapFunction<String, Tuple2<Integer, String>>) value -> {
            String[] words = value.split(",");                                                 // words = [ {1} {John}]
            return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
        });

        //locationSet = tuple of (1  DC)
        DataSet<Tuple2<Integer, String>> locationSet = env.readTextFile(params.get("input2")).
                map((MapFunction<String, Tuple2<Integer, String>>) value -> {
                    String[] words = value.split(",");
                    return new Tuple2<>(Integer.parseInt(words[0]), words[1]);
                });

        // right outer join datasets on person_id
        // joined format will be <id, person_name, state>

        DataSet<Tuple3<Integer, String, String>> joined = personSet.rightOuterJoin(locationSet).where(0).equalTo(0)
                .with((JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>) (person, location) -> {
                    // check for nulls
                    if (person == null) {
                        return new Tuple3<>(location.f0, "NULL", location.f1);
                    }

                    return new Tuple3<>(person.f0, person.f1, location.f1);
                });//.collect();

        joined.writeAsCsv(params.get("output"), "\n", " ");

        env.execute("Right Outer Join Example");
    }

}
