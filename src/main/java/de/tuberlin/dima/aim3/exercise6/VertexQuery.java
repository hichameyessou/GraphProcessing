package de.tuberlin.dima.aim3.exercise6;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.graph.ReduceEdgesFunction;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;
import org.apache.flink.util.Collector;

import java.util.Iterator;

public class VertexQuery {

    DataSet<Tuple3<Long, Long, Boolean>> maxFriends;
    DataSet<Tuple3<Long, Long, Boolean>> maxFoe;

    public VertexQuery(Graph<Long, NullValue, Boolean> g){
        this.maxFriends = g.getEdgesAsTuple3();
        this.maxFoe = g.getEdgesAsTuple3();

        ComputeAndSave();
    }

    private void ComputeAndSave() {
        maxFriends
                .filter(new FFilterFriends())
                .groupBy(0)
                .reduceGroup(new EdgeGroupReducer())
                .maxBy(1)
                .writeAsCsv(Config.outputPath()+"vertex_max_friend.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        maxFoe.filter(new FFilterFoe())
                .groupBy(0)
                .reduceGroup(new EdgeGroupReducer())
                .maxBy(1)
                .writeAsCsv(Config.outputPath()+"vertex_max_foe.txt", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
    }

    private class FFilterFriends implements org.apache.flink.api.common.functions.FilterFunction<Tuple3<Long, Long, Boolean>> {
        @Override
        public boolean filter(Tuple3<Long, Long, Boolean> in) throws Exception {
            return in.f2;
        }
    }
    private class FFilterFoe implements org.apache.flink.api.common.functions.FilterFunction<Tuple3<Long, Long, Boolean>> {
        @Override
        public boolean filter(Tuple3<Long, Long, Boolean> in) throws Exception {
            return !in.f2;
        }
    }

    private class EdgeGroupReducer implements GroupReduceFunction<Tuple3<Long, Long, Boolean>, Tuple2<Long, Long>> {
        @Override
        public void reduce(Iterable<Tuple3<Long, Long, Boolean>> iterable, Collector<Tuple2<Long, Long>> collector) throws Exception {
            Iterator<Tuple3<Long, Long, Boolean>> iterator = iterable.iterator();
            Long vertex = iterator.next().f0;
            long count = 1L;
            while (iterator.hasNext()) {
                iterator.next();
                count++;
            }
            collector.collect(new Tuple2<Long, Long>(vertex, count));
        }
    }
}
