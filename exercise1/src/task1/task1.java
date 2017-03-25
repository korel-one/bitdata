package task1;

/**
 * Created by Sergii on 11.03.2017.
 */

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;

public class task1 {

    public static int ip_to_int(String ip_address) {
        return Integer.parseInt(ip_address.split("\\.")[0]);
    }

    public static class Query {
        Query(int time, int ip, int window_size) {
            this.time = time;
            this.ip = ip;
            this.window_size = window_size;
        }

        int time, ip, window_size;
    }

    public static class QueryScheduler {
        private ArrayBlockingQueue<Query> queries;

        private ArrayList<Integer> time;
        private ArrayList<Integer> ip;
        private ArrayList<Integer> window_size;

        QueryScheduler() {
            queries = new ArrayBlockingQueue<Query>(1000);
        }

        public void add(int time, int ip, int window_size) {
            queries.add(new Query(time, ip, window_size));
        }

        public boolean hasQuery(int time) {
            if(queries.isEmpty()) {
                return false;
            }
            return queries.peek().time == time;
        }

        public Query getQuery() {
            return queries.poll();
        }
    }

    public static void main(String[] args) {

        File tsv_file = new File("file1.tsv");
        File q_file = new File("task1_queries.txt");

        JumpingWindow jw = new JumpingWindow(10000, 0.01);
        QueryScheduler query_scheduler = new QueryScheduler();

        ArrayList<Integer> output = null;
        try {
            Scanner sc_q = new Scanner(q_file);
            while (sc_q.hasNextLine()) {
                String[] dataArray = sc_q.nextLine().split("\\s+");

                Integer v1 = Integer.parseInt(dataArray[0]);
                Integer v2 = Integer.parseInt(dataArray[1].split("\\.")[0]);
                Integer v3 = Integer.parseInt(dataArray[2]);
                query_scheduler.add(v1, v2, v3);
            }
            sc_q.close();

            Scanner sc_tsv = new Scanner(tsv_file);

            int time = 0;
            output = new ArrayList<Integer>();
            while (sc_tsv.hasNextLine()) {
                String[] dataArray = sc_tsv.nextLine().split("\t");
                jw.insertEvent(ip_to_int(dataArray[0]));

                if (query_scheduler.hasQuery(time++)) {
                    Query q = query_scheduler.getQuery();
                    output.add(jw.getFreqEstimation(q.ip, q.window_size));
                }

            }
            sc_tsv.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        if (output != null) {
            BufferedWriter br = null;
            try {
                br = new BufferedWriter(new FileWriter("out1.txt"));

                StringBuffer sb = new StringBuffer();
                for (int i = 0; output != null && i < output.size()-1; i++) {
                    sb.append(output.get(i).toString());
                    sb.append(",");
                }
                sb.append(output.get(output.size()-1));
                br.write(sb.toString());
                br.close();
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
    }
}
