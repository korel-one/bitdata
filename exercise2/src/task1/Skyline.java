package task1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by Sergii on 25.03.2017.
 */

import java.io.FileReader;
import java.io.File;

public class Skyline {

    public static ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions){
        ArrayList<Tuple> list = new ArrayList<Tuple>();
        for(ArrayList<Tuple> p: partitions) {
            list.addAll(p);
        }

        return list;
    }

    static ArrayList<ArrayList<Tuple>> skylines;
    private static ArrayList<Tuple> dcSkyline_aux(ArrayList<Tuple> inputList
            , int left, int right, int blockSize) {

        if((right - left + 1) <= blockSize) {
            return nlSkyline(new ArrayList<Tuple>(inputList.subList(left, right)));
            //return bnlSkyline(new ArrayList<Tuple>(inputList.subList(left, right)));
        }
        else {
            int N = inputList.size();
            skylines.add(dcSkyline_aux(inputList, 0, N/2-1, blockSize));
            skylines.add(dcSkyline_aux(inputList, N/2, N-1, blockSize));

            return mergePartitions(skylines);
        }
    }

    public static ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int blockSize) {
        skylines = new ArrayList<ArrayList<Tuple>>();
        ArrayList<Tuple> list = dcSkyline_aux(inputList, 0, inputList.size()-1, blockSize);
        if(list.size() < inputList.size()) {
            return dcSkyline(list, blockSize);
        }
        else {
            return list;
        }
    }

    public static ArrayList<Tuple> dcSkyline_iter(ArrayList<Tuple> inputList, int blockSize){
        // compute the skyline of a given collection of data
        // partition size - number of elements in a partition

        //1. divide the problem to sub-problems of the same type
        int N = inputList.size()%blockSize == 0 ? inputList.size()/blockSize : inputList.size()/blockSize + 1;

        ArrayList<ArrayList<Tuple>> blocks = new ArrayList<ArrayList<Tuple>>();

        for(int i = 0; i < N; i+=blockSize ) {
            int end_index = (i + blockSize) > inputList.size() ?
                inputList.size() : i + blockSize;

            blocks.add(new ArrayList<Tuple>(inputList.subList(i, end_index)));
        }

        //2. solve recursively:       compute the skyline of each partition using BNL
        ArrayList<ArrayList<Tuple>> skylines = new ArrayList<ArrayList<Tuple>>();
        for(ArrayList<Tuple> partition: blocks) {
            skylines.add(nlSkyline(partition));
        }



        //3. merge partial solutions: compute the union of all partial skylines

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

    public static ArrayList<Tuple> bnlSkyline(ArrayList<Tuple> partition) {
        ArrayList<Tuple> window = new ArrayList<Tuple>();

        Iterator<Tuple> p_it = partition.iterator();
        while (p_it.hasNext()) {
            if (window.isEmpty()) {
                window.add(p_it.next());
                p_it.remove();
                continue;
            }

            Tuple obj = p_it.next();

            Iterator<Tuple> win_it = window.iterator();
            boolean win_dominates = false;
            while (win_it.hasNext()) {
                Tuple win_obj = win_it.next();

                if(obj.isIncomparable(win_obj)) {
                    continue;
                }

                //object dominates -> remove candidate from window of incomparable objects
                if (obj.dominates(win_obj)) {
                    win_it.remove();
                }
                //object is dominated -> exit inner loop
                else if (win_obj.dominates(obj)) {
                    p_it.remove();
                    win_dominates = true;
                    break;
                }
            }

            if (!win_dominates) {
                window.add(obj);
                p_it.remove();
            }
        }
        return window;
    }

    void test_nl() {
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

    void test_bnl() {
        ArrayList<Tuple> data = new ArrayList<Tuple>();
        data.add(new Tuple(250, 14));
        data.add(new Tuple(600, 15));
        data.add(new Tuple(2100, 9));
        data.add(new Tuple(9900, 3));
        data.add(new Tuple(1000, 9));
        data.add(new Tuple(9700, 3));

        data = bnlSkyline(data);

        for(Tuple t: data) {
            System.out.println(String.format("%d, %d", t.getPrice(), t.getAge()));
        }
    }

    public static void main(String[] args) {
        //(new Skyline()).test_nl();
        //(new Skyline()).test_bnl();


        String csvFile = "car.csv"
                , delimiter = "\\|";
        Scanner scanner = null;

        ArrayList<Tuple> data = null;
        try{
            scanner = new Scanner(new File(csvFile));

            data = new ArrayList<Tuple>();
            while(scanner.hasNext()) {
                String[] dataArray = scanner.nextLine().split(delimiter);
                int price = Integer.parseInt(dataArray[0]);
                int age = Integer.parseInt(dataArray[1]);

                data.add(new Tuple(price, age));
            }
            scanner.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Skyline skyline = new Skyline();
        ArrayList<Tuple> res = skyline.dcSkyline(data, 100);
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
