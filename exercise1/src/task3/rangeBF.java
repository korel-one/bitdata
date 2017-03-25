package task3;

import task2.betterFrequencyEstimator;

import java.nio.channels.IllegalBlockingModeException;
import java.util.*;
import java.io.*;

/**
 * Created by Sergii on 14.03.2017.
 */
public class rangeBF {

    static long calcStep(int level) {
        return 1L << level;
    }

    static long calcIndex(long value, int level) {
        return value/calcStep(level);
    }

    private class coverage {

        class DyadicInterval extends Object {
            long l, r;
            int level = 0;

            DyadicInterval(long l, long r, int level) {
                this.l = l;
                this.r = r;
                this.level = level;
            }

            //.hashCode() is a simple hash over the object's reference address;
            //.equals() is true if and only if both objects are the same reference (ie, o1 == o2).

            @Override
            public int hashCode() {
                final int prime = 31;
                return prime * (int)(l + r);
            }

            @Override
            public boolean equals(Object obj) {
                if(this == obj) {
                    return true;
                }
                DyadicInterval di = (DyadicInterval)obj;
                return  l == di.l && r == di.r;
            }
        }

        long l, r;
        Set<DyadicInterval> dyadicCoverage;

        boolean subInterval(long left, long right) {
            return left >= l && right <= r;
        }

        //bottom-up from left bound
        private void goUpStartLeft(){
            long left = l, right = l;
            int level = 0;

            while(level <= MAX_LEVEL && subInterval(left, right)) {
                if(left == l && right == r) {
                    //System.out.println(String.format("[%d, %d]", left, right));
                    dyadicCoverage.add( new DyadicInterval(left, right, 32-level));
                    break;
                }

                long index = calcIndex(left, level);
                long step = calcStep(level);

                long curr_left = step*index;
                long curr_right = curr_left + step - 1;

                if(curr_left == left){
                    if(subInterval(curr_left, curr_right)) {
                        right = curr_right;
                        ++level;
                    }
                    else {
                        dyadicCoverage.add(new DyadicInterval(left, right, 32-level));
                        //System.out.println(String.format("[%d, %d]", left, right));
                        break;
                    }
                }
                else {
                    //System.out.println(String.format("[%d, %d]", left, right));
                    dyadicCoverage.add(new DyadicInterval(left, right, 32-level));

                    left = (index + 1)*step;
                    right = left + step - 1;
                    ++level;
                }
            }

            if(left == right) {
                dyadicCoverage.add(new DyadicInterval(left, right, 32-level));
                //System.out.println(String.format("[%d, %d]", left, right));
            }
        }

        //bottom-up from right bound
        private void goUpStartRight() {
            long left = r, right = r;
            int level = 0;

            while(level <= 31 && subInterval(left, right)) {
                if(left == this.l && right == this.r) {
                    dyadicCoverage.add(new DyadicInterval(left, right, 32-level));
                    //System.out.println(String.format("[%d, %d]", left, right));
                    break;
                }

                long index = calcIndex(right, level);
                long step = calcStep(level);

                long curr_left = step*index;
                long curr_right = curr_left + step - 1;

                if(curr_right == right){
                    if(subInterval(curr_left, curr_right)) {
                        left = curr_left;
                        ++level;
                    }
                    else {
                        //System.out.println(String.format("[%d, %d]", left, right));
                        dyadicCoverage.add(new DyadicInterval(left, right, 32-level));
                        break;
                    }
                }
                else {
                    //System.out.println(String.format("[%d, %d]", left, right));
                    dyadicCoverage.add(new DyadicInterval(left, right, 32-level));

                    left = (index + 1)*step;
                    right = left + step - 1;
                    ++level;
                }
            }

            if(left == right) {
                //System.out.println(String.format("[%d, %d]", left, right));
                dyadicCoverage.add(new DyadicInterval(left, right, 32-level));
            }
        }

        Set<DyadicInterval> decompose(long l, long r) {
            this.l = l;
            this.r = r;

            dyadicCoverage = new HashSet<DyadicInterval>();

            goUpStartLeft();
            goUpStartRight();

            return dyadicCoverage;
        }
    }

    coverage dyadicCoverage;

    static final int MAX_LEVEL = Integer.SIZE - 1;

    static long toLong(int key) {
        return ((long)key) & 0x00000000ffffffffL;
    }

    private class HashFunc {
        HashFunc(int seed1, int seed2, int range){
            a = seed1;
            b = seed2;
            this.range = range;
        }

        /* (a*x + b) mod (2^p-1), where a,b are random seeds, different for each hash function, which
        are positive and can range up to Integer.MAX_VALUE, x is the element that you insert and
        p is a big prime number. * */
        long apply(long x) {

            // prevents overflow
            return (((((a%q)*(x%q))%q + b%q)%q)%range + range)%range;
        }

        private long a, b // random seeds
                , range; // [0.. range]
    }

    private static final int q = (int)(Math.pow(2., 31.)-1);// prime
    private static int getSeed(Random random) { return (random.nextInt()%q + q)%q; }
    private double NETWORK_POWER = 400000.d;

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
        Iterator<Integer> it1 = seeds_1.iterator()
                , it2 = seeds_2.iterator();

        for(int i = 0; i < d; ++i) {
            hf[i] = new HashFunc(it1.next(), it2.next(), range);
        }
        return hf;
    }

    private class BloomFilter {
        private BitSet sketch;
        private HashFunc[] hash_func;
        private int size;

        BloomFilter(int m, int k) {
            sketch = new BitSet(m);
            size = m;
            hash_func = generateHashFunctions(k, m);
        }

        public void insert(long key) {
            for(HashFunc hf: hash_func) {
                sketch.set((int)(hf.apply(key)));
            }
        }

        public boolean contain(long key) {
            for(HashFunc hf: hash_func) {
                if(!sketch.get((int)(hf.apply(key))))
                    return false;
            }
            return true;
        }
    }

    BloomFilter[] generateRangeBloomFilters(int N, double pr) {
        int curr_universe = 1;

        BloomFilter[] bloomFilters = new BloomFilter[N];
        for(int i = 0; i < N; ++i) {
            curr_universe <<= i;
            double universe = Math.min(curr_universe, NETWORK_POWER);

            double prob = 1.0 - Math.pow((1.0 - pr), 1.0/(64));

            // bloom filter's bit array length
            int m = (int) Math.ceil( ( -Math.log(prob)*universe / (Math.log(2)*Math.log(2)) ));

            // bloom filter's pairwise independent hash functions
            int k = (int) Math.ceil(Math.log(2) * (double) m / universe);
            bloomFilters[i] = new BloomFilter(m, k);
        }
        return bloomFilters;
    }
    BloomFilter[] bloomFilters;

    public rangeBF(double pr) {
        bloomFilters = generateRangeBloomFilters(Integer.SIZE, pr);
        dyadicCoverage = new coverage();
    }

    void insertValue(int key) {
        long long_key = toLong(key);

        int N = bloomFilters.length;
        for(int level = 0; level < N; ++level) {
            long index = calcIndex(long_key, MAX_LEVEL-level);
            bloomFilters[level].insert(index);
        }
    }

    boolean existsInRange(int l, int r) {
        long ll = toLong(l);
        long lr = toLong(r);
        if(ll > lr) {
            return false;
        }

        Set<coverage.DyadicInterval> intervals = dyadicCoverage.decompose(ll, lr);

        System.out.println(String.format(" decompose [%d, %d]", ll, lr));
        for(coverage.DyadicInterval i : intervals) {
            System.out.println(String.format("[%d, %d], %d", i.l, i.r, i.level));
        }

        for(coverage.DyadicInterval i : intervals) {
            long index = calcIndex(i.l, MAX_LEVEL - i.level);
            if(bloomFilters[i.level].contain(index)) {
                return true;
            }
        }
        return false;
    }
}
