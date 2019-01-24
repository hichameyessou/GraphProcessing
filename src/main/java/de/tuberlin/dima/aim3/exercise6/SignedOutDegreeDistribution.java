package de.tuberlin.dima.aim3.exercise6;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class SignedOutDegreeDistribution {
    DataSet<Tuple2<Long, LongValue>> friendOutDegrees ;
    DataSet<Tuple2<Long,LongValue>> foeOutDegrees ;
    DataSet<Long> totVertices;

    public SignedOutDegreeDistribution(Graph<Long, NullValue, Boolean> g, DataSet<Long> v){
        this.friendOutDegrees = g.filterOnEdges(new FilterFriends()).outDegrees();
        this.foeOutDegrees = g.filterOnEdges(new FilterNotFriends()).outDegrees();
        this.totVertices = v;
        ComputeAndSave();
    }

    private void ComputeAndSave() {
        friendOutDegrees
                .groupBy(1).reduceGroup(new DegreeDistribution.DistributionElement())
                .withBroadcastSet(totVertices, "totVertices")
                .writeAsCsv(Config.outputPath()+" out-degree_friend_dist.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);;

        foeOutDegrees
                .groupBy(1).reduceGroup(new DegreeDistribution.DistributionElement())
                .withBroadcastSet(totVertices, "totVertices")
                .writeAsCsv(Config.outputPath()+" out-degree_foe_dist.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

    }
}

class FilterFriends implements FilterFunction<Edge<Long, Boolean>> {
    @Override
    public boolean filter(Edge<Long, Boolean> longBooleanEdge) throws Exception {
        return longBooleanEdge.f2 == true;
    }
}

class FilterNotFriends implements FilterFunction<Edge<Long, Boolean>> {
    @Override
    public boolean filter(Edge<Long, Boolean> longBooleanEdge) throws Exception {
        return longBooleanEdge.f2 == false;
    }
}
