package com.atguigu;

import elki.data.Cluster;
import elki.data.Clustering;
import elki.data.NumberVector;
import elki.database.Database;
import elki.database.relation.Relation;
import org.javatuples.Pair;

import java.util.HashMap;
import java.util.List;

public class myCluster {
    public static Clustering<?> clusteringResult;
    public long timestamp;  // 添加时间戳属性
    public static Relation<NumberVector> relation;
    public HashMap<Integer,Integer> pre_pos;
    public Integer idd;
    public Database database;
    List<List<Pair<NumberVector,Integer>>> clusters;

    public myCluster(Clustering<?> clusteringResult1,Relation<NumberVector> relation1,HashMap<Integer,Integer> pre_pos,List<List<Pair<NumberVector,Integer>>> clusters1) {
        clusters=clusters1;
        clusteringResult=clusteringResult1;
        relation=relation1;
        timestamp=0;
        this.pre_pos=pre_pos;
    }

    public static Clustering<?> getClusteringResult() {
        return clusteringResult;
    }

    public static void setClusteringResult(Clustering<?> clusteringResult) {
        myCluster.clusteringResult = clusteringResult;
    }

    public static Relation<NumberVector> getRelation() {
        return relation;
    }

    public static void setRelation(Relation<NumberVector> relation) {
        myCluster.relation = relation;
    }
}
