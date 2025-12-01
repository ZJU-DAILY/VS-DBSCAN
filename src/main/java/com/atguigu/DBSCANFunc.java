package com.atguigu;

import elki.clustering.dbscan.DBSCAN;
import elki.data.Cluster;
import elki.data.Clustering;
import elki.data.NumberVector;
import elki.data.type.TypeUtil;
import elki.database.Database;
import elki.database.StaticArrayDatabase;
import elki.database.ids.DBIDIter;
import elki.database.ids.DBIDRef;
import elki.database.relation.Relation;
import elki.datasource.ArrayAdapterDatabaseConnection;
import elki.distance.minkowski.EuclideanDistance;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static java.lang.Math.min;

public class DBSCANFunc {

    double eps;
    int minPoints;
    double[][] dataPoints;
    Iterable<data> elements;
    HashMap<Integer,Integer> pre_pos;
    List<data> list;


    public DBSCANFunc(double eps, int minPoints, Iterable<data> elements) {
        this.eps = eps;
        this.minPoints = minPoints;
        this.elements = elements;

        // 1. 收集 Iterable<data> 到 List<data>
        list = new ArrayList<>();
        for (data d : elements) {
            list.add(d);
        }

        // 2. 根据列表大小申请 dataPoints 数组
        dataPoints = new double[list.size()][];

        // 3. 把每个 data 的 attributes 填入 dataPoints
        for (int i = 0; i < list.size(); i++) {
            dataPoints[i] = list.get(i).getAttributes();
        }
    }


    public myCluster dbscanCluster() {

        ArrayAdapterDatabaseConnection dataConnection = new ArrayAdapterDatabaseConnection(dataPoints);
        Database database = new StaticArrayDatabase(dataConnection, null);
        database.initialize();

        // ===== 3. DBSCAN参数 =====
        DBSCAN<NumberVector> dbscan = new DBSCAN<>(
                EuclideanDistance.STATIC,  // 距离计算方式
                eps,                       // epsilon 值
                minPoints                  // minpts 值
        );
        Relation<NumberVector> relation =
                database.getRelation(TypeUtil.NUMBER_VECTOR_FIELD);
        Clustering<?> clusteringResult = dbscan.run(relation);

        Integer min_id=999999999;
        for (Cluster<?> c : clusteringResult.getAllClusters()) {
            DBIDIter iter = c.getIDs().iter();
            while (iter.valid()) {
                DBIDRef id = (DBIDRef) iter;
                int idx = id.internalGetIndex() - 1;
                min_id=min(min_id,idx);
                iter.advance();
            }
        }

        pre_pos=new HashMap<>();
        for (Cluster<?> c : clusteringResult.getAllClusters()) {
            DBIDIter iter = c.getIDs().iter();
            while (iter.valid()) {
                DBIDRef id = (DBIDRef) iter;
                int idx = id.internalGetIndex() - 1;
                pre_pos.put(idx,list.get(idx-min_id).pos);
                NumberVector v = relation.get(iter);
                iter.advance();
            }
        }

        List<List<Pair<NumberVector,Integer>>> clusters = new ArrayList<>();
        int cls=0;
        for (Cluster<?> cl : clusteringResult.getAllClusters()) {
            cls++;
            List<Pair<NumberVector,Integer>> points = new ArrayList<>();
            // 获取该簇的所有 DBID，然后用迭代器遍历
            DBIDIter iter = cl.getIDs().iter();
            while (iter.valid()) {
                DBIDRef id = (DBIDRef) iter;
                int idx = id.internalGetIndex() - 1;
                if(pre_pos.get(idx)!=null){
                    idx=pre_pos.get(idx);
                }else{
                }
                // DBIDIter 本身实现了 DBIDRef，可以直接传给 relation.get()
                NumberVector v = relation.get(iter);
                Pair<NumberVector,Integer> now_p=Pair.with(v,idx);
                points.add(now_p);
                iter.advance();
            }
            clusters.add(points);
        }
        return new myCluster(clusteringResult,relation,pre_pos,clusters);
    }

}