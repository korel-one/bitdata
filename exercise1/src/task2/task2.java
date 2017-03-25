package task2;

import task1.task1;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * Created by Sergii on 11.03.2017.
 */
public class task2 {

    public static int ipToInt(String ip_address) {
        String[] ip = ip_address.split("\\.");

        return (Integer.parseInt(ip[0]) << 24)
                + (Integer.parseInt(ip[1]) << 16)
                + (Integer.parseInt(ip[2]) << 8)
                + (Integer.parseInt(ip[3]));
    }

    public static class Query {
        Query(int time, int ip) {
            this.time = time;
            this.ip = ip;
        }

        int time, ip;
    }

    public static class QueryScheduler {
        private ArrayBlockingQueue<Query> queries;

        QueryScheduler() {
            queries = new ArrayBlockingQueue<Query>(1000);
        }

        public void add(int time, int ip) {
            queries.add(new Query(time, ip));
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
        File q_file = new File("task2_queries.txt");

        betterFrequencyEstimator bfe = new betterFrequencyEstimator(10000000
                , 0.1f, 0.01f, 0.9f);
        QueryScheduler query_scheduler = new QueryScheduler();

        ArrayList<Integer> output = null;
        try {
            Scanner sc_q = new Scanner(q_file);
            while (sc_q.hasNextLine()) {
                String line = sc_q.nextLine();
                if(!line.isEmpty()) {
                    String[] dataArray = line.split("\\s+");

                    int time = Integer.parseInt(dataArray[0]);
                    int ip = ipToInt(dataArray[1]);
                    query_scheduler.add(time, ip);
                }
            }
            sc_q.close();

            Scanner sc_tsv = new Scanner(tsv_file);
            int time = 0;
            output = new ArrayList<Integer>();
            while (sc_tsv.hasNextLine()) {
                String line = sc_tsv.nextLine();
                if(!line.isEmpty()) {
                    String[] dataArray = line.split("\t");

                    int ip = ipToInt(dataArray[0]);
                    bfe.addArrival(ip);

                    if (query_scheduler.hasQuery(time++)) {
                        Query q = query_scheduler.getQuery();
                        output.add(bfe.getFreqEstimation(q.ip));
                    }
                }
            }
            sc_tsv.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        if (output != null) {
            BufferedWriter br = null;
            try {
                br = new BufferedWriter(new FileWriter("out2.txt"));

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
