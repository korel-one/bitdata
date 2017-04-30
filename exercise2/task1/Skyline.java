package task1;

/**
 * Created by Sergii on 25.03.2017.
 */

import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.*;

public class Skyline {

    ArrayList<Tuple> mergePartitions(ArrayList<ArrayList<Tuple>> partitions) {

        if(partitions.size() == 1) {
            return nlSkyline(partitions.get(0));
        }

        int N = partitions.size();

        ArrayList<Tuple> left = mergePartitions(new ArrayList<ArrayList<Tuple>>(partitions.subList(0, N/2)));
        ArrayList<Tuple> right = mergePartitions(new ArrayList<ArrayList<Tuple>>(partitions.subList(N/2, N)));

        left.addAll(right);
        Collections.sort(left, (t1, t2)-> {
            int p1 = t1.getPrice();
            int p2 = t2.getPrice();
            return (p1 - p2 != 0) ? p1 - p2 : t1.getAge() - t1.getAge();
        });
        return nlSkyline(left);
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
        int N = inputList.size();
        ArrayList<ArrayList<Tuple>> partitions = IntStream.range(0, (N - 1)/blockSize + 1)
                        .mapToObj( i -> new ArrayList<Tuple>(inputList.subList(i *= blockSize
                                    , (N - blockSize >= i) ? i + blockSize : N)))
                        .collect(Collectors.toCollection(ArrayList::new));


        return mergePartitions(partitions);
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