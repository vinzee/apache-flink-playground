package p1;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class SplitOperator {
    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.readTextFile("/home/jivesh/oddeven");

        SplitStream<Integer> evenOddStream = text.map((MapFunction<String, Integer>) Integer::parseInt)
                .split((OutputSelector<Integer>) value -> {
                    List<String> out = new ArrayList<String>();
                    if (value % 2 == 0)
                        out.add("even");              // label element  --> even 454   odd 565 etc
                    else
                        out.add("odd");
                    return out;
                });

        DataStream<Integer> evenData = evenOddStream.select("even");
        DataStream<Integer> oddData = evenOddStream.select("odd");

        evenData.writeAsText("/home/jivesh/even");
        oddData.writeAsText("/home/jivesh/odd");

        // execute program
        env.execute("ODD EVEN");
    }
}
    
  
