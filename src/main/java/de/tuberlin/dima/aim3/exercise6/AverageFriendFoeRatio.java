package de.tuberlin.dima.aim3.exercise6;

import com.google.common.collect.Iterables;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.Vertex;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class AverageFriendFoeRatio {

    DataSet<Vertex<Long, NullValue>> vertices;
    DataSet<Tuple3<Long, Long, Boolean>> graphWithEdges;
    DataSet<Tuple3<Long, Long, Boolean>> counterGraphWithEdges ;
    DataSet<Long> numVertices;
    ExecutionEnvironment env;

    public AverageFriendFoeRatio(Graph<Long, NullValue, Boolean> g, ExecutionEnvironment e){
        this.graphWithEdges = g.getEdgesAsTuple3();
        this.counterGraphWithEdges = g.getEdgesAsTuple3();
        this.env = e;

        ComputeAndSave();
    }

    private void ComputeAndSave() {

        try {
            Long x = counterGraphWithEdges
                    .groupBy(0)
                    .reduceGroup(new FriendsToFoeRatio())
                    .count();
            System.out.println("NUMBER OF VERTICES WITH FRIENDS AND FOE: "+x);
            numVertices = env.fromElements(x);
        } catch (Exception e) {
            e.printStackTrace();
        }

        graphWithEdges
            .groupBy(0)
            .reduceGroup(new FriendsToFoeRatio())
            .reduce(new AverageReducer())
            .map(new AvgMapper())
            .withBroadcastSet(numVertices, "totVertices")
            .writeAsCsv(Config.outputPath()+"avg_ratio.txt", FileSystem.WriteMode.OVERWRITE)
            .setParallelism(1);
    }

    private class FriendsToFoeRatio implements GroupReduceFunction<Tuple3<Long, Long, Boolean>,Tuple2<Long, Double>> {
        @Override
        public void reduce(Iterable<Tuple3<Long, Long, Boolean>> iterable, Collector<Tuple2<Long, Double>> collector) throws Exception {
            Iterator<Tuple3<Long, Long, Boolean>> iterator = iterable.iterator();
            int friendsCount = 0;
            int foeCount = 0;
            Tuple3<Long, Long, Boolean> firstVertex = iterator.next();

            if (firstVertex.f2)
                friendsCount++;
            else
                foeCount++;

            while (iterator.hasNext()) {
                if (iterator.next().f2)
                    friendsCount++;
                else
                    foeCount++;
            }
            if(friendsCount != 0 && foeCount != 0)
                collector.collect(new Tuple2<Long, Double>(firstVertex.f0, (double)friendsCount/foeCount));
        }
    }

    private class AverageReducer implements ReduceFunction<Tuple2<Long, Double>> {

        @Override
        public Tuple2<Long, Double> reduce(Tuple2<Long, Double> one, Tuple2<Long, Double> two) throws Exception {
            return new Tuple2<Long, Double>(0L,one.f1+two.f1);
        }
    }

    private class AvgMapper extends RichMapFunction<Tuple2<Long, Double>, Tuple2<Long, Double>> {
        private long totVertices;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            totVertices = getRuntimeContext().<Long>getBroadcastVariable("totVertices").get(0);
            System.out.println("totVertices: " + totVertices);
        }

        @Override
        public Tuple2<Long, Double> map(Tuple2<Long, Double> in) throws Exception {
            return new Tuple2<Long,Double>(0L, in.f1/totVertices);
        }
    }

    private class EdgeFilter implements FilterFunction<org.apache.flink.graph.Edge<Long, Boolean>> {
        @Override
        public boolean filter(Edge<Long, Boolean> longBooleanEdge) throws Exception {
            System.out.println("FILTER EDGE: "+longBooleanEdge);
            return true;
        }
    }
}
