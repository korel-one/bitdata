package task1;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;

/**
 * Created by Sergii on 25.03.2017.
 */

import java.io.File;
import java.util.stream.Collectors;

public class Skyline {

    private ArrayList<ArrayList<Tuple>> skylines = new ArrayList<ArrayList<Tuple>>();

    ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions) {
        ArrayList<Tuple> list = partitions.stream()
                .flatMap(ArrayList<Tuple>::stream)
                .collect(Collectors.toCollection(ArrayList<Tuple>::new));

        // clear partitions container after merging
        partitions.clear();

        Collections.sort(list, (t1, t2)-> {
            int p1 = t1.getPrice();
            int p2 = t2.getPrice();
            return (p1 - p2 != 0) ? p1 - p2 : t1.getAge() - t1.getAge();
        });

        return list;
    }

    ArrayList<Tuple> dcSkyline(ArrayList<Tuple> inputList, int partitionSize) {
        ArrayList<Tuple> list = dcSkylineAux(inputList, partitionSize);
        if(list.size() < inputList.size()) {
            return dcSkyline(list, partitionSize);
        }
        else {
            return list;
        }
    }

    private ArrayList<Tuple> dcSkylineAux(ArrayList<Tuple> inputList, int blockSize) {

        if(inputList.size() <= blockSize) {
            return nlSkyline(inputList);
        }

        int N = inputList.size();
        skylines.add(dcSkylineAux(new ArrayList<Tuple>(inputList.subList(0, (N / 2) + 1)), blockSize));
        skylines.add(dcSkylineAux(new ArrayList<Tuple>(inputList.subList(N/2+1, N)), blockSize));

        return mergePartitions(skylines);
    }

    ArrayList<Tuple> nlSkyline(ArrayList<Tuple> partition) {
        return bnlSkyline(partition, 0, partition.size()-1);
    }

    private ArrayList<Tuple> bnlSkyline(ArrayList<Tuple> partition, int left, int right) {
        ArrayList<Tuple> window = new ArrayList<Tuple>(Arrays.asList(partition.get(left)));

        for (int i = left+1; i <= right; ++i) {
            Tuple obj = partition.get(i);

            Iterator<Tuple> win_it = window.iterator();
            boolean win_dominates = false;
            while (win_it.hasNext()) {
                Tuple win_obj = win_it.next();

                if (obj.isIncomparable(win_obj)) {
                    continue;
                }

                //object dominates -> remove candidate from window of incomparable objects
                if (obj.dominates(win_obj)) {
                    win_it.remove();
                }
                //object is dominated -> exit inner loop
                else if (win_obj.dominates(obj)) {
                    win_dominates = true;
                    break;
                }
            }

            if (!win_dominates) {
                window.add(obj);
            }
        }
        return window;
    }

    public static void main(String[] args) {

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

        try {
            Skyline skyline = new Skyline();
            ArrayList<Tuple> res1 = skyline.dcSkyline(data, 100);

            ArrayList<Tuple> res2 = skyline.dcSkyline(data, 100);

            ArrayList<Tuple> res3 = skyline.bnlSkyline(data, 0, data.size()-1);
            Collections.sort(res3, (t1, t2)->{int p1 = t1.getPrice();
                int p2 = t2.getPrice();
                return (p1 - p2 != 0) ? p1 - p2 : t1.getAge() - t1.getAge();});

            System.out.println();
        }
        catch(Exception e) {
            e.printStackTrace();
        }
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
        return (price < other.price) && (age <= other.age) || (price <= other.price) && (age < other.age);
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