package task3;

import task2.task2;

import java.io.*;
import java.util.ArrayList;
import java.util.Scanner;

/**
 * Created by Sergii on 14.03.2017.
 */
public class task3 {

    public static int ipToInt(String ip_address) {
        String[] ip = ip_address.split("\\.");

        return (Integer.parseInt(ip[0]) << 24)
                + (Integer.parseInt(ip[1]) << 16)
                + (Integer.parseInt(ip[2]) << 8)
                + (Integer.parseInt(ip[3]));
    }

    public static void main(String[] args) {

        double pr = 0.1;
        rangeBF rbf = new rangeBF(pr);
        File tsv_file = new File("file1.tsv");

        try {
            Scanner sc_tsv = new Scanner(tsv_file);

            int counter = 0;
            while (sc_tsv.hasNextLine()) {
                ++counter;

                String line = sc_tsv.nextLine();
                if(!line.isEmpty()) {
                    String[] dataArray = line.split("\t");
                    rbf.insertValue(ipToInt(dataArray[0]));
                }
                if(counter%100 == 0) {
                    System.out.println(counter);
                }
            }
            sc_tsv.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        // read and collect all queries
        File q_file = new File("task3_queries.txt");
        ArrayList<Integer> ip_start = null, ip_end = null;
        try {
            Scanner sc_q = new Scanner(q_file);

            ip_start = new ArrayList<Integer>();
            ip_end = new ArrayList<Integer>();

            while (sc_q.hasNextLine()) {
                String line = sc_q.nextLine();
                if (!line.isEmpty()) {
                    String[] dataArray = line.split("\\s+");

                    int ip1 = ipToInt(dataArray[0]);
                    int ip2 = ipToInt(dataArray[1]);

                    ip_start.add(ip1);
                    ip_end.add(ip2);
                }
            }
            sc_q.close();
        }
        catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        int queries_number = ip_end.size();
        Boolean[] output = new Boolean[queries_number];

        for (int i = 0; i < queries_number; ++i) {
            int min = ip_start.get(i);
            int max = ip_end.get(i);
            output[i] = rbf.existsInRange(min, max);
        }

        BufferedWriter br = null;
        try {
            br = new BufferedWriter(new FileWriter("out3.txt"));
            StringBuffer sb = new StringBuffer();
            for (int i = 0; i < queries_number-1; i++) {
                sb.append(output[i].toString());
                sb.append(",");
            }
            sb.append(output[queries_number-1]);
            br.write(sb.toString());
            br.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }
}
