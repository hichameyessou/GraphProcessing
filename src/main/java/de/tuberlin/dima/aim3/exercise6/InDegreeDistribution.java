package de.tuberlin.dima.aim3.exercise6;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.graph.Graph;
import org.apache.flink.types.LongValue;
import org.apache.flink.types.NullValue;

public class InDegreeDistribution {
    DataSet<Tuple2<Long, LongValue>> inDegrees;
    DataSet<Tuple2<Long,LongValue>> outDegrees;
    DataSet<Long> totVertices;

    public InDegreeDistribution(Graph<Long, NullValue, Boolean> g, DataSet<Long> v){
        this.inDegrees = g.inDegrees();
        this.outDegrees = g.outDegrees();
        this.totVertices = v;
        ComputeAndSave();
    }

    public void ComputeAndSave(){

        DataSet<Tuple2<Long, Double>> inDegreeDistribution = inDegrees
                .groupBy(1).reduceGroup(new DegreeDistribution.DistributionElement())
                .withBroadcastSet(totVertices, "totVertices");

        DataSet<Tuple2<Long, Double>> outDegreeDistribution = outDegrees
                .groupBy(1).reduceGroup(new DegreeDistribution.DistributionElement())
                .withBroadcastSet(totVertices, "totVertices");

        inDegreeDistribution
                .writeAsCsv(Config.outputPath()+"in-degree_dist.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);

        outDegreeDistribution
                .writeAsCsv(Config.outputPath()+"out-degree_dist.csv", FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1);
    }
}
