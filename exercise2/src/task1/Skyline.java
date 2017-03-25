package task1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

/**
 * Created by Sergii on 25.03.2017.
 */

import java.io.FileReader;
import java.io.File;

public class Skyline {
    ArrayList<Tuple> data;
    Skyline() {
        data = new ArrayList<Tuple>();
    }


    public static ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions){
        // merges the skyline of different partitions



        // TODO
        return null;
    }


    public static ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int blockSize){
        // compute the skyline of a given collection of data
        // partition size - number of elements in a partition


        // TODO
        return null;
    }

    static int nextCandidateIndex(boolean[] processed) {
        int i = 0;
        while(i < processed.length && processed[i]) {++i;}

        return i == processed.length ? -1 : i;
    }

    public static ArrayList<Tuple> nlSkyline(ArrayList<Tuple> partition) {
        if(partition.isEmpty()) {
            return null;
        }

        ArrayList<Tuple> res = new ArrayList<Tuple>();
        boolean[] processed = new boolean[partition.size()];

        int candidate_index = 0
                , cardinality = 0;

        Tuple candidate = partition.get(candidate_index);
        processed[candidate_index] = true;
        ++cardinality;

        while(cardinality < processed.length) {

            int i = 0;
            for(; i < processed.length; ++i) {
                if(!processed[i]) {
                    Tuple curr_tuple = partition.get(i);
                    if(candidate.isIncomparable(curr_tuple)) {
                        continue;
                    }

                    if(candidate.dominates(curr_tuple)) {
                        processed[i] = true;
                        ++cardinality;
                    }
                    else {
                        candidate = curr_tuple;
                        if(!processed[candidate_index]) {
                            processed[candidate_index] = true;
                            ++cardinality;
                        }

                        candidate_index = i;
                        break;
                    }
                }
            }

            if(i == processed.length) {
                //System.out.println(String.format("%d, %d", candidate.getPrice(), candidate.getAge()));
                res.add(candidate);

                int index = nextCandidateIndex(processed);
                if(index != -1) {
                    candidate = partition.get(index);
                    candidate_index = index;

                    processed[candidate_index] = true;
                    ++cardinality;
                }
            }
        }

        return res;
    }

    void test_nestedloop() {
        ArrayList<Tuple> data = new ArrayList<Tuple>();
        data.add(new Tuple(250, 14));
        data.add(new Tuple(600, 15));
        data.add(new Tuple(2100, 9));
        data.add(new Tuple(9900, 3));
        data.add(new Tuple(1000, 9));
        data.add(new Tuple(9700, 3));

        data = nlSkyline(data);

        for(Tuple t: data) {
            System.out.println(String.format("%d, %d", t.getPrice(), t.getAge()));
        }
    }

    public static void main(String[] args) {
        (new Skyline()).test_nestedloop();


//        String csvFile = "car.csv"
//                , delimiter = "\\|";
//        Skyline skyline = null;
//        Scanner scanner = null;
//
//        try{
//            scanner = new Scanner(new File(csvFile));
//
//            skyline = new Skyline();
//            while(scanner.hasNext()) {
//                String[] dataArray = scanner.nextLine().split(delimiter);
//                int price = Integer.parseInt(dataArray[0]);
//                int age = Integer.parseInt(dataArray[1]);
//
//                Tuple tuple = new Tuple(price, age);
//                skyline.data.add(tuple);
//            }
//        }
//        catch (FileNotFoundException e) {e.printStackTrace();}
//        finally { if(scanner != null) {scanner.close();}}
    }
}

class Tuple {
    private int price;
    private int age;

    public Tuple(int price, int age){
        this.price = price;
        this.age = age;
    }

    public boolean dominates(Tuple other){



        return (price <= other.price) && (age <=other.age);

        // TODO
        //return false;
    }

    public boolean isIncomparable(Tuple other){

        return !dominates(other) && !other.dominates(this);
    }

    public int getPrice() {
        return price;
    }

    public int getAge() {
        return age;
    }

    public String toString(){
        return price + "," + age;
    }

    public boolean equals(Object o) {
        if(o instanceof Tuple) {
            Tuple t = (Tuple)o;
            return this.price == t.price && this.age == t.age;
        } else {
            return false;
        }
    }
}
