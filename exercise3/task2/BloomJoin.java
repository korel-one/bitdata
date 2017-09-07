import java.io.*;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class BloomJoin {
    public static void main(String[] args) {
        ArrayList<R> r = new ArrayList<>();
        ArrayList<S> s = new ArrayList<>();
        for(int i = 0; i<100; i++) {
            r.add(new R(i * 2, i * 3));
            s.add(new S(i * 3, i * 4));
        }

        NodeA n1 = new NodeA(r);
        Node n2 = new NodeB(s);
        n1.run(n2);
        for(RS rs: n1.getResult()) {
            System.out.println(rs);
        }
        System.out.println(n1.getTotalMessageSize());
        System.out.println(n2.getTotalMessageSize());
    }
}

abstract class Node {
    public static final int BUCKETS = 10000;
    private long totalMessageSize;
    private Message inbox;

    /**
     * @param receiver the receiver node.
     * @param msg the message to be sent to the receiver node.
     */
    protected void send(Node receiver, Object msg) {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(msg);
            oos.close();
            Message message = new Message(this, receiver, baos.toByteArray());
            totalMessageSize += baos.size();
            receiver.setInbox(message);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    protected void setInbox(Message msg) {
        this.inbox = msg;
    }

    /**
     * @return the last message that delivered to the current node.
     */
    protected Message getInbox() {
        return inbox;
    }

    /**
     * @return the total number of bytes that is sent from the current node.
     */
    public long getTotalMessageSize() {
        return totalMessageSize;
    }
    /**
     * The hash function that should be used by BloomJoin
     */
    public int hashFunction(int i) {
        return i % BUCKETS;
    }
    public abstract void run(Node other);
}

class NodeA extends Node {

    private List<R> data;
    private List<RS> result;

    public NodeA(List<R> data) {
        this.data = data;
    }

    public void run(Node other) {
        //1. create a bloom filter
        BitSet sketch = new BitSet(BUCKETS);

        /* use hashFunction to construct bloom filter*/
        data.stream().forEach(tuple -> sketch.set(tuple.getB()));

        //2. send it to NodeB
        this.send(other, sketch);

        //3. wait until NodeB sends relevant tuples
        other.run(this);

        result = new ArrayList<RS>();
        Map<Integer, List<Integer>> relevantTuples = (Map<Integer, List<Integer>>)getInbox().getContent();

        if(relevantTuples.isEmpty()) {
            return;
        }

        //4. perform local join
        for(R tuple: data) {
            int a = tuple.getA();
            int b = tuple.getB();
            if(relevantTuples.containsKey(b)) {
                for (Integer c : relevantTuples.get(b)) {
                    result.add(new RS(a, b, c));
                }
            }
        }
    }

    /**
     * @return the result of BloomJoin.
     */
    public List<RS> getResult() {
        return result;
    }
}
class NodeB extends Node {
    private List<S> data;
    public NodeB(List<S> data) {
        this.data = data;
    }
    public void run(Node other) {

        //1. use bloom filter (filter relevant S tuples)
        BitSet bloomFilter = (BitSet)this.getInbox().getContent();
        Map<Integer, List<Integer>> relevantTuples = data.stream()
                .filter(tpl -> bloomFilter.get(hashFunction(tpl.getB())))
                .collect(Collectors.groupingBy(S::getB,
                        Collectors.mapping(S::getC, Collectors.toList())));

        //2. send filtered tuples back to NodeA
        this.send(other, relevantTuples);
    }
}

class R {
    private int a, b;
    public R(int a, int b) {
        this.a = a;
        this.b = b;
    }

    public int getA() {
        return a;
    }

    public int getB() {
        return b;
    }
}

class S implements Serializable {
    private int b, c;
    public S(int b, int c) {
        this.b = b;
        this.c = c;
    }

    public int getB() {
        return b;
    }

    public int getC() {
        return c;
    }
}

class RS {
    private int a, b, c;
    public RS(int a, int b, int c) {
        this.a = a;
        this.b = b;
        this.c = c;
    }

    public int getA() {
        return a;
    }

    public int getB() {
        return b;
    }

    public int getC() {
        return c;
    }

    @Override
    public String toString() {
        return "<" + a + ", " + b + ", " + c + ">";
    }

    @Override
    public boolean equals(Object obj) {
        if(obj instanceof RS) {
            RS rs = (RS) obj;
            return a == rs.a && b == rs.b && c == rs.c;
        } else {
            return false;
        }
    }
}


class Message {
    private Node sender;
    private Node receiver;
    private byte[] msg;
    public Message(Node sender, Node receiver, byte[] msg) {
        this.sender = sender;
        this.receiver = receiver;
        this.msg = msg;
    }

    /**
     * @return the object that the message contains.
     */
    public Object getContent() {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(msg);
            ObjectInputStream ois = new ObjectInputStream(bis);
            return ois.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}