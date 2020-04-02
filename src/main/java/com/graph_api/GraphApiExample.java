package com.graph_api;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.graph.library.SingleSourceShortestPaths;
import org.apache.flink.types.NullValue;

public class GraphApiExample {
    private static final String user1Name = "Vipul";

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        /* format: user, friend */
        DataSet<Tuple2<String, String>> friends = env.readTextFile("/home/jivesh/graphnew")
                .map((MapFunction<String, Tuple2<String, String>>) value -> {
                    String[] words = value.split("\\s+");
                    return new Tuple2<>(words[0], words[1]);
                });

        /* prepare normal dataset to edges for graph */
        DataSet<Edge<String, NullValue>> edges = friends.map((MapFunction<Tuple2<String, String>, Edge<String, NullValue>>) value -> {
            Edge<String, NullValue> edge = new Edge<>();
            edge.setSource(value.f0); // user
            edge.setTarget(value.f1); // friend

            // return an edge between user and friend
            return edge;
        });

        /* create graph from edges dataset */
        Graph<String, NullValue, NullValue> friendsGraphs =
                Graph.fromDataSet(edges, env);

        Graph<String, NullValue, Double> weightedfriendsGraph =
                friendsGraphs.mapEdges((MapFunction<Edge<String, NullValue>, Double>) edge -> 1.0);

        /* get all friend of friends of friends of....*/
        SingleSourceShortestPaths<String, NullValue> s1 =
                new SingleSourceShortestPaths<>(user1Name, 10);

        DataSet<Vertex<String, Double>> result =
                s1.run(weightedfriendsGraph);

        /* get only friends of friends for Vipul */
        DataSet<Vertex<String, Double>> fOfUser1 =
                result.filter((FilterFunction<Vertex<String, Double>>) value -> value.f1 == 2);

        fOfUser1.print();

        env.execute("Graph API Example");
    }

}

