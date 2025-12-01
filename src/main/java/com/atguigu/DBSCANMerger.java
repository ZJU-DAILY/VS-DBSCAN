package com.atguigu;

import elki.data.Clustering;
import elki.data.DoubleVector;
import elki.data.NumberVector;
import elki.data.model.Model;
import elki.data.Cluster;
import elki.database.ids.DBIDIter;
import elki.database.ids.DBIDRef;
import elki.database.relation.Relation;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
public class DBSCANMerger {

    // 从 ELKI 的 Clustering + Relation 抽取出每个簇的成员向量列表
    public static List<List<Pair<NumberVector,Integer>>> extractClusters(
            Clustering<?> clustering,
            Relation<NumberVector> relation,
            HashMap<Integer,Integer> pre_pos,
            int idd,
            long timestamp ) {
        List<List<Pair<NumberVector,Integer>>> clusters = new ArrayList<>();
        System.out.println(idd);
        System.out.println(timestamp);
        System.out.println(' ');
//        System.out.println(pre_pos.toString());
        // 遍历每个 ELKI 簇

        for (Cluster<?> cl : clustering.getAllClusters()) {
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
                System.out.println(v.toString());
                Pair<NumberVector,Integer> now_p=Pair.with(v,idx);
                points.add(now_p);
                iter.advance();
            }
            clusters.add(points);
        }
        return clusters;
    }


    /**
     * 合并两组聚类：
     * 对每对簇（list1 中的簇 vs. list2 中未合并簇），若满足 shouldMerge，则把 list2 的簇并入 list1
     */
    public static List<List<Pair<NumberVector,Integer>>> merge(
            List<List<Pair<NumberVector,Integer>>> list1,
            List<List<Pair<NumberVector,Integer>>> list2,
            double eps) {
        boolean[] used2 = new boolean[list2.size()];
        // 对 list1 中每个簇，尝试合并 list2 中尚未被合并的簇
        for (List<Pair<NumberVector,Integer>> c1 : list1) {
            for (int j = 0; j < list2.size(); j++) {
                if (used2[j]) continue;
                List<Pair<NumberVector,Integer>> c2 = list2.get(j);
                if (shouldMerge(c1, c2, eps)) {
                    c1.addAll(c2);
                    used2[j] = true;
                }
            }
        }
        // 把 list2 中剩余的簇也加入结果
        for (int j = 0; j < list2.size(); j++) {
            if (!used2[j]) {
                list1.add(list2.get(j));
            }
        }
        return list1;
    }

    /** 判断两个簇的合并条件：max(LAb, LBa) ≤ eps */
    public static boolean shouldMerge(
            List<Pair<NumberVector,Integer>> c1,
            List<Pair<NumberVector,Integer>> c2,
            double eps) {
        NumberVector cent1 = computeCentroid(c1);
        NumberVector cent2 = computeCentroid(c2);
        if(cent1==null){
            if(cent2==null){
                return true;
            }else{
                return true;
            }
        }else {
            if(cent2==null){
                return true;
            }
        }
        double minAtoB = Double.MAX_VALUE;
        for (Pair<NumberVector,Integer> p : c1) {
            double d = euclidean(p.getValue0(), cent2);
            if (d < minAtoB) minAtoB = d;
        }
        double minBtoA = Double.MAX_VALUE;
        for (Pair<NumberVector,Integer> p : c2) {
            double d = euclidean(p.getValue0(), cent1);
            if (d < minBtoA) minBtoA = d;
        }
        return Math.max(minAtoB, minBtoA) <= eps;
    }

    /** 计算向量列表的质心（算术平均） */
    public static NumberVector computeCentroid(List<Pair<NumberVector,Integer>> pts) {
        if (pts == null || pts.isEmpty()) {
            // 如果 pts 是空的，可以抛出异常或返回一个默认的值
            return null;
        }
        int dim = pts.get(0).getValue0().getDimensionality();
        double[] sum = new double[dim];
        for (Pair<NumberVector,Integer> vv : pts) {
            NumberVector v = vv.getValue0();
            for (int i = 0; i < dim; i++) {
                sum[i] += v.doubleValue(i);
            }
        }
        for (int i = 0; i < dim; i++) {
            sum[i] /= pts.size();
        }
        return new DoubleVector(sum);
    }

    /** 计算欧几里得距离 */
    private static double euclidean(NumberVector v1, NumberVector v2) {
        int dim = v1.getDimensionality();
        double s = 0;
        for (int i = 0; i < dim; i++) {
            double diff = v1.doubleValue(i) - v2.doubleValue(i);
            s += diff * diff;
        }
        return Math.sqrt(s);
    }
}