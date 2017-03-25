package task3;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by Sergii on 19.03.2017.
 */
public class coverage {
    static long calcStep(int level) {
        return 1L << level;
    }

    static long calcIndex(long value, int level) {
        return value/calcStep(level);
    }

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
            boolean res = l == di.l && r == di.r;
            return res;
        }
    }

    long l, r;
    Set<DyadicInterval> dyadicCoverage = new HashSet<DyadicInterval>();

    coverage(long l, long r) {
        this.l = l;
        this.r = r;
    }

    boolean subInterval(long left, long right) {
        return left >= l && right <= r;
    }

    //bottom-up
    void goUpStartLeft(){
        long left = l, right = l;
        int level = 0;

        while(level <= 31 && subInterval(left, right)) {
            if(left == this.l && right == this.r) {
                //System.out.println(String.format("[%d, %d]", left, right));
                dyadicCoverage.add(new DyadicInterval(left, right, 32-level));
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


    void goUpStartRight() {
        long left = r, right = r;
        int level = 0;
        int max_level = 31;

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

    public static void main(String[] args) {
        //coverage c = new coverage(1, 15);
        coverage c = new coverage(28753322, 29081060);
        c.goUpStartLeft();
        c.goUpStartRight();

        for(DyadicInterval i: c.dyadicCoverage) {
            System.out.println(String.format("[%d, %d], %d", i.l, i.r, i.level));
        }
    }
}
