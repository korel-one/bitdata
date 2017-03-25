package task2;

/**
 * Created by Sergii on 11.03.2017.
 */

import java.lang.*;
import java.util.*;


public class betterFrequencyEstimator {

    private static final int q = (int)(Math.pow(2., 31.)-1);// prime
    private static int getSeed(Random random) { return (random.nextInt()%q + q)%q; }

    private class HashFunc {
        HashFunc(int seed1, int seed2, int range){
            a = seed1;
            b = seed2;
            this.range = range;
        }

        /* (a*x + b) mod (2^p-1), where a,b are random seeds, different for each hash function, which
        are positive and can range up to Integer.MAX_VALUE, x is the element that you insert and
        p is a big prime number. * */
        int apply(int x) {

            // prevents overflow
            return (((((a%q)*(x%q))%q + b%q)%q)%range + range)%range;
        }

        private int a, b // random seeds
                , range; // [0.. range]
    }

    private HashFunc[] generateHashFunctions(int d, int range) {
        Random random = new Random();

        Set<Integer> seeds_1 = new HashSet<Integer>();
        while(seeds_1.size() < d) {
            seeds_1.add(getSeed(random));
        }

        Set<Integer> seeds_2 = new HashSet<Integer>();
        while(seeds_2.size() < d) {
            seeds_2.add(getSeed(random));
        }

        HashFunc[] hf = new HashFunc[d];

        Iterator<Integer> it1 = seeds_1.iterator();
        Iterator<Integer> it2 = seeds_2.iterator();
        for(int i = 0; i < d; ++i) {
            hf[i] = new HashFunc(it1.next(), it2.next(), range);
        }
        return hf;
    }

    private class BloomFilter {
        private BitSet sketch;
        private HashFunc[] hash_func;

        BloomFilter(int m, int k) {
            sketch = new BitSet(m);
            hash_func = generateHashFunctions(k, m);
        }
        public void insert(int key) {
            for(HashFunc hf: hash_func) {
                sketch.set(hf.apply(key));
            }
        }

        public boolean contain(int key) {
            for(HashFunc hf: hash_func) {
                if(!sketch.get(hf.apply(key)))
                    return false;
            }
            return true;
        }
    }

//    private class CS422 {
//        private BitSet sketch;
//        private HashFunc hash_func;
//
//        CS422(int m) {
//            Random random = new Random();
//            hash_func = new HashFunc(getSeed(random), getSeed(random), m);
//            sketch = new BitSet(m);
//        }
//
//        public void insert(int key) {
//            sketch.set(hash_func.apply(key));
//        }
//        public boolean contain(int key) {
//            return sketch.get(hash_func.apply(key));
//        }
//    }

    private class CountMin {

        private HashFunc[] hash_func;
        private int[][] sketch;
        private BloomFilter bloom_filter;

        public CountMin(int d, int w, BloomFilter bloomFilter) {
            bloom_filter = bloomFilter;

            sketch = new int[d][w];
            hash_func = generateHashFunctions(d, w);
        }

        public void update(int key) {
            bloom_filter.insert(key);

            for(int i = 0; i < hash_func.length; ++i) {
                ++sketch[i][hash_func[i].apply(key)];
            }
        }

        public int getFrequency(int key) {

            if(!bloom_filter.contain(key)){
                return 0;
            }

            int min_fr = Integer.MAX_VALUE;
            for(int i = 0; i < hash_func.length; ++i) {
                min_fr = Math.min(min_fr, sketch[i][hash_func[i].apply(key)]);
            }

            return min_fr;
        }
    }

    private CountMin count_min;

    public betterFrequencyEstimator(int availableSpace
            , float pr1, float epsilon, float pr2) throws OutOfMemoryError
    {
        float delta = 1 - pr2;

        int d = (int)Math.ceil(Math.log(1/delta));
        int w = (int)Math.ceil(Math.E/epsilon);

        int universe = 400000
                , hash_func_size = Integer.BYTES*3;

        // bloom filter's bit array length
        int m = (int) Math.ceil( ( -Math.log(pr1)*universe / (Math.log(2)*Math.log(2)) ));
        // bloom filter's pairwise independent hash functions
        int k = (int) Math.ceil(Math.log(2) * (double) m / (double) universe);

        int consumed_size = (m/Byte.SIZE + k*hash_func_size) // bloom sketch size
                + d*w*Integer.BYTES  // CM sketch size
                + d*hash_func_size; // HashFunc array size

        if(consumed_size > availableSpace) {
            throw new OutOfMemoryError("Specified probability doesn't fit available memory");
        }

        count_min = new CountMin(d, w, new BloomFilter(m, k));
    }

    void addArrival(int key) {
        count_min.update(key);
    }

    int getFreqEstimation(int key) {
        return count_min.getFrequency(key);
    }
}