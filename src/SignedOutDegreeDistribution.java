package de.tuberlin.dima.aim3.exercise6;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Edge;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class SignedOutDegreeDistribution {
    DataSet<Tuple2<Long, LongValue>> friendOutDegrees;
    DataSet<Tuple2<Long,LongValue>> foeOutDegrees;

    DataSet<Long> totFriendVertices;
    DataSet<Long> totFoeVertices;
    ExecutionEnvironment env;

    public SignedOutDegreeDistribution(Graph<Long, NullValue, Boolean> g, ExecutionEnvironment e){
        this.friendOutDegrees = g.filterOnEdges(new FilterFriends()).outDegrees();
        this.foeOutDegrees = g.filterOnEdges(new FilterNotFriends()).outDegrees();
        this.env = e;

        ComputeAndSave();
    }

    private void ComputeAndSave() {
        try {
            totFriendVertices = env.fromElements(friendOutDegrees.count());
            totFoeVertices = env.fromElements(foeOutDegrees.count());
        } catch (Exception e) {
            e.printStackTrace();
        }

        friendOutDegrees
            .groupBy(1)
            .reduceGroup(new DegreeDistribution.DistributionElement())
            .withBroadcastSet(totFriendVertices, "totVertices")
            .writeAsCsv(Config.outputPath()+" out-degree_friend_dist.csv", FileSystem.WriteMode.OVERWRITE)
            .setParallelism(1);;

        foeOutDegrees
            .groupBy(1)
            .reduceGroup(new DegreeDistribution.DistributionElement())
            .withBroadcastSet(totFoeVertices, "totVertices")
            .writeAsCsv(Config.outputPath()+" out-degree_foe_dist.csv", FileSystem.WriteMode.OVERWRITE)
            .setParallelism(1);

    }
}

class FilterFriends implements FilterFunction<Edge<Long, Boolean>> {
    @Override
    public boolean filter(Edge<Long, Boolean> longBooleanEdge) throws Exception {
        return longBooleanEdge.f2;
    }
}

class FilterNotFriends implements FilterFunction<Edge<Long, Boolean>> {
    @Override
    public boolean filter(Edge<Long, Boolean> longBooleanEdge) throws Exception {
        return !longBooleanEdge.f2;
    }
}
