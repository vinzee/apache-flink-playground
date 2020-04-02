package com.datastream_api;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class SplitOperator {
    public static void main(String[] args) throws Exception {
        // set up the stream execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // make parameters available in the web interface
        env.getConfig().setGlobalJobParameters(params);

        DataStream<String> text = env.readTextFile("/home/jivesh/oddeven");

        // this needs to be an anonymous inner class, so that we can analyze the type
        final OutputTag<String> evenOutputTag = new OutputTag<String>("even-side-output") {
        };
        final OutputTag<String> oddOutputTag = new OutputTag<String>("odd-side-output") {
        };

        SingleOutputStreamOperator<Integer> allNumbers = text.process(new ProcessFunction<String, Integer>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Integer> out) {
                // emit data to regular output
                int val = Integer.parseInt(value);
                out.collect(val);

                // emit data to side output
                if (val % 2 == 0)
                    ctx.output(evenOutputTag, "sideout-" + value);
                else
                    ctx.output(oddOutputTag, "sideout-" + value);
            }
        });

        DataStream<String> evenNumbers = allNumbers.getSideOutput(evenOutputTag);
        DataStream<String> oddNumbers = allNumbers.getSideOutput(oddOutputTag);

        allNumbers.print();
        evenNumbers.print();
        oddNumbers.print();

        // execute program
        env.execute("ODD EVEN");
    }
}


