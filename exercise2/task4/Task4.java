package task4;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction2;
import scala.Tuple2;

import java.util.*;

/**
 * Created by Sergii on 25.04.17.
 */

public class Task4 {

    static Integer C_CUSTKEY = 0;
    static Integer O_COMMENT = 8;
    static Integer O_CUSTKEY = 1;

    public static void main(String[] args) {
        String relation1 = args[0];
        String relation2 = args[1];
        String output = args[2];

        SparkConf conf = new SparkConf().setMaster("local").setAppName("task4");
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            JavaRDD<String> r1 = sc.textFile(relation1);//customer
            JavaRDD<String> r2 = sc.textFile(relation2);//orders

            //load the primary key column: 0-index
            JavaRDD<String> primaryKey = r1.map(line -> line.split("\\|")[C_CUSTKEY]);
            JavaPairRDD<String, String> indexedR1 =
                    primaryKey.mapToPair(id -> new Tuple2<String, String>(id, ""));

            //load the foreign key column: 1-index
            JavaRDD<String> foreignKey = r2.map(line -> line.split("\\|")[O_CUSTKEY]);
            JavaRDD<String> comment = r2.map(line -> line.split("\\|")[O_COMMENT]);
            JavaPairRDD<String, String> indexedR2 = foreignKey.zip(comment);

            int partitions = Math.max(r1.partitions().size(), r2.partitions().size());

            //repartition
            JavaPairRDD<String, String> rddR1 = indexedR1.partitionBy(new HashPartitioner(partitions));
            JavaPairRDD<String, String> rddR2 = indexedR2.partitionBy(new HashPartitioner(partitions));

            FlatMapFunction2<Iterator<Tuple2<String, String>>
                    , Iterator<Tuple2<String, String>>, String> func = (it1, it2) -> {

                Map<Integer, String> hs = new HashMap<Integer, String>();
                while (it1.hasNext()) {
                    Tuple2<String, String> data = it1.next();
                    hs.put(data._1.hashCode(), data._1);
                }

                List<String> localJoin = new ArrayList<String>();
                while (it2.hasNext()) {
                    Tuple2<String, String> data = it2.next();
                    if (hs.containsKey(data._1.hashCode())) {
                        String hData = hs.get(data._1.hashCode());
                        if (hData.compareTo(data._1) == 0) {
                            localJoin.add(hData + "," + data._2);
                        }
                    }
                }

                return localJoin;
            };

            JavaRDD<String> resRDD = rddR1.zipPartitions(rddR2, func);
            resRDD.coalesce(1).saveAsTextFile(output);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sc.stop();
        }
    }
}