package task1;

/**
 * Created by Sergii on 11.03.2017.
 */

import org.omg.CORBA.INTERNAL;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.concurrent.ArrayBlockingQueue;

public class JumpingWindow {
    private int sub_window_size = 0;
    private int window_size = 0;
    private int counter = 0;

    private int[] recent_window = new int[256];
    private int[] partially_expiring_window = new int[256];

    private ArrayBlockingQueue<int[]> window;

    private void cleanRecentWindow() {
        recent_window = new int[256];
        Arrays.fill(recent_window, 0);
    }

    private Iterator<int[]> advance(int shift) {
        int curr_shift = 0;

        Iterator<int[]> it = window.iterator();
        while(curr_shift++ < shift && it.hasNext()) {
            it.next();
        }

        return it;
    }

    private int relevantExpiringWindowWeight(int srcIP, int queryWindowSizeW1) {

        int fi_packets = queryWindowSizeW1 - counter;
        boolean expired_sub_window = fi_packets % sub_window_size != 0;

        int shift = expired_sub_window ? window_size - (fi_packets/sub_window_size + 1) :
                window_size - fi_packets/sub_window_size;

        Iterator<int[]> it = advance(shift-1);

        int exp_sw_weight = it.next()[srcIP];
        int weight = expired_sub_window ? exp_sw_weight/2 : exp_sw_weight;

        while(it.hasNext()) {
            weight += it.next()[srcIP];
        }

        return weight + recent_window[srcIP];
    }

    public JumpingWindow(int windowSizeW, double epsilon) {
        sub_window_size = (int)(2*epsilon*windowSizeW);
        window_size = windowSizeW/sub_window_size - 1;
        window = new ArrayBlockingQueue<int[]>(window_size);
    }

    void insertEvent(int srcIP) {
        if(counter == sub_window_size) {
            counter = 0;

            if(window.remainingCapacity() == 0) {
                partially_expiring_window = window.poll();
            }

            window.add(recent_window);
            cleanRecentWindow();
        }

        ++recent_window[srcIP];
        ++counter;
    }

    int getFreqEstimation(int srcIP, int queryWindowSizeW1) {
        return queryWindowSizeW1 == 0 ? getFreqEstimation(srcIP) :
                relevantExpiringWindowWeight(srcIP, queryWindowSizeW1);
    }

    int getFreqEstimation(int srcIP){
        int weight = recent_window[srcIP];
        if(counter < sub_window_size) {
            weight += partially_expiring_window[srcIP]/2;
        }

        for(int[] freq_array: window) {
            weight += freq_array[srcIP];
        }

        return weight;
    }
}
