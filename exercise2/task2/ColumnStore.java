package task2;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.function.BiFunction;
import java.util.Map;
import java.util.HashMap;
import java.lang.*;

/**
 * Created by Sergii on 24.04.2017.
 */

public class ColumnStore {

    static class QueryPlan {

        private class Schema {
            Schema(String schema) {
                indexOf = new HashMap<String, Integer>();
                typeOf = new HashMap<String, String>();

                String[] data = schema.split(",");
                int i = 0;
                for (String item : data) {
                    String[] attrType = item.split(":");
                    String attribute = attrType[0];
                    String type = attrType[1];

                    indexOf.put(attribute, i++);
                    typeOf.put(attribute, type);
                }
            }

            Integer getAttrIndex(String attribute) {
                return indexOf.get(attribute);
            }

            String getAttrType(String attribute) {
                return typeOf.get(attribute);
            }

            private Map<String, String> typeOf;
            private Map<String, Integer> indexOf;
        }

        private class WhereClause {
            WhereClause(String expression){ //attr|op|value e.g. attr1|=|3,attr2|<|7
                String[] data = expression.split("\\|");
                attr = data[0];
                op = data[1];
                value = data[2];
            }
            String attr;
            String op;
            String value;
        }

        QueryPlan(String[] projectionList, String[] whereList, String schemaStr) {
            this.projectionList = projectionList;
            this.whereList = Arrays.stream(whereList)
                    .map(where -> new WhereClause(where))
                    .toArray(size -> new WhereClause[size]);

            schema = new Schema(schemaStr);
        }

        private JavaRDD<Long> evaluateEx(JavaRDD<String> table, int i) {
            if(i == whereList.length) {
                return table.zipWithIndex().map(elem -> elem._2);
            }

            WhereClause condition = whereList[i];
            Integer index = schema.getAttrIndex(condition.attr);
            JavaPairRDD<String, Long> indexedColumn =
                    table.map(line -> line.split(",")[index.intValue()]) //column JavaRDD<String>
                    .zipWithIndex();

            String type = schema.getAttrType(condition.attr);
            String op = condition.op;
            String value = condition.value;

            JavaPairRDD<String, Long> filteredColumn = indexedColumn.filter(elem -> {
                return compare(type, op, elem._1, value);
            });

            JavaRDD<Long> ids = filteredColumn.map(elem -> elem._2);
            return ids.intersection(evaluateEx(table, i+1));
        }

        //evaluate WHERE clause
        JavaRDD<Long> evaluate(JavaRDD<String> table){
            return evaluateEx(table, 0);
        }

        private JavaPairRDD<Long, String> projectEx(JavaRDD<String> table
                , JavaPairRDD<Long, String> ids, int i) {
            if(i == projectionList.length) {
                return ids;
            }

            int index = schema.getAttrIndex(projectionList[i]).intValue();
            JavaPairRDD<Long, String> indexedColumn =
                    table.map(line -> line.split(",")[index]).zipWithIndex()
                    .mapToPair(pair -> pair.swap());

            JavaPairRDD<Long, String> joined = ids.join(indexedColumn)
                    .mapValues(val -> {
                        if(val._1.isEmpty())
                            return val._2;
                        return String.join(",", val._1, val._2);
                    });

            return projectEx(table, joined, i+1);
        }

        JavaPairRDD<Long, String> project(JavaRDD<String>table, JavaPairRDD<Long, String> ids) {
            return projectEx(table, ids, 0);
        }

        private String[] projectionList;
        private WhereClause[] whereList;
        private Schema schema;
    }

    static Map<String, BiFunction<String, String, Boolean>> compareStr;
    static Map<String, BiFunction<Integer, Integer, Boolean>> compareInteger;
    static Map<String, BiFunction<Float, Float, Boolean>> compareFloat;

    static private Boolean compare(String type, String op, String val1, String val2) {
        if(type.compareTo("String") == 0) {
            if(compareStr == null) {
                compareStr = new HashMap<String, BiFunction<String, String, Boolean>>();
                compareStr.put("=", (str1, str2)->str1.compareTo(str2) == 0);
                compareStr.put(">", (str1, str2)->str1.compareTo(str2) > 0);
                compareStr.put("<", (str1, str2)->str1.compareTo(str2) < 0);
                compareStr.put(">=", (str1, str2)->str1.compareTo(str2) >= 0);
                compareStr.put("<=", (str1, str2)->str1.compareTo(str2) <= 0);
            }
            return compareStr.get(op).apply(val1, val2);
        } else if(type.compareTo("Float") == 0) {
            if(compareFloat == null) {
                compareFloat = new HashMap<String, BiFunction<Float, Float, Boolean>>();
                compareFloat.put("=", (f1, f2)-> f1.floatValue() == f2.floatValue());
                compareFloat.put(">", (f1, f2)-> f1.floatValue() > f2.floatValue());
                compareFloat.put("<", (f1, f2)-> f1.floatValue() < f2.floatValue());
                compareFloat.put(">=", (f1, f2)-> f1.floatValue() >= f2.floatValue());
                compareFloat.put("<=", (f1, f2)-> f1.floatValue() <= f2.floatValue());
            }
            return compareFloat.get(op).apply(Float.valueOf(val1), Float.valueOf(val2));
        } else { //assume only Integer is possible
            if(compareInteger == null) {
                compareInteger = new HashMap<String, BiFunction<Integer, Integer, Boolean>>();
                compareInteger.put("=", (i1, i2)-> i1.intValue() == i2.intValue());
                compareInteger.put(">", (i1, i2)-> i1.intValue() >  i2.intValue());
                compareInteger.put("<", (i1, i2)-> i1.intValue() <  i2.intValue());
                compareInteger.put(">=",(i1, i2)-> i1.intValue() >= i2.intValue());
                compareInteger.put("<=",(i1, i2)-> i1.intValue() <= i2.intValue());
            }
            return compareInteger.get(op).apply(Integer.valueOf(val1), Integer.valueOf(val2));
        }
    }

    public static void main(String[] args) {
        String inCsv = args[0]
                , outCsv = args[1]
                , schemaStr = args[2]
                , projectionList = args[3]
                , whereList = args[4];

        QueryPlan qp = new QueryPlan(projectionList.split(",")
                , whereList.split(","), schemaStr);

        SparkConf conf = new SparkConf().setMaster("local").setAppName("columnStore");
        JavaSparkContext sc = new JavaSparkContext(conf);

        try {
            JavaRDD<String> input = sc.textFile(inCsv);
            JavaRDD<Long> ids = qp.evaluate(input);

            JavaPairRDD<Long, String> idsAux = ids.mapToPair(id -> new Tuple2<Long, String>(id, ""));

            //project from indices
            JavaPairRDD<Long, String> indexedProject = qp.project(input, idsAux);
            JavaRDD<String> res = indexedProject.sortByKey().map(val -> val._2);

            //one partition
            res.coalesce(1).saveAsTextFile(outCsv);
        }
        catch(Exception e) {
            e.printStackTrace();
        }
        finally{
            sc.stop();
        }
    }
}