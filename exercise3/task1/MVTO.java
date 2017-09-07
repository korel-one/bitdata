import java.util.*;
import java.util.concurrent.TransferQueue;
import java.util.stream.Collectors;

/**
 *  implement a (main-memory) data store with MVTO.
 *  objects are <int, int> key-value pairs.
 *  if an operation is to be refused by the MVTO protocol,
 *  undo its xact (what work does this take?) and throw an exception.
 *  garbage collection of versions is not required.
 *  Throw exceptions when necessary, such as when we try to execute an operation in
 *  a transaction that is not running; when we insert an object with an existing
 *  key; when we try to read or write a nonexisting key, etc.
 *  Keep the interface, we want to test automatically!
 *
 **/

class Version extends Object {
  Version(int value, int xact) {
    WTS = xact;
    RTS = xact;
    this.value = value;
  }

  int WTS //timestamp of a transaction that created the version
    , RTS //largest timestamp of transaction that read the version
    , value;
}

public class MVTO {

  //transaction is blocked by other transactions
  static Map<Integer, Set<Integer>> blockedByTransactions = new Hashtable<Integer, Set<Integer>>();
  static Map<Integer, Set<Integer>> selfBlockedTransactions = new Hashtable<Integer, Set<Integer>>();

  //key, sequence of versions
  static Map<Integer, LinkedList<Version>> versions = new HashMap<Integer, LinkedList<Version>>();
  static Set<Integer> abortedTransactions = new HashSet<Integer>();

  static Set<Integer> pendingTransations = new HashSet<Integer>();

  private static int max_xact = 0;

  static boolean isActive(int xact) {
    // double use of (to track block dependencies,
    // and if transaction is running - it's id is registered as a key in blockedByTransactions
    return blockedByTransactions.containsKey(xact);
  }

  static void checkTransaction(int xact) throws Exception {
    if (!isActive(xact)) {
      throw new Exception(String.format("Non-active transaction:%d", xact));
    }
  }

  static void checkKey(int key, int xact, boolean present) throws Exception {
    if (present != versions.containsKey(key)) {
      rollback(xact);
      throw new Exception(String.format("key %d is %s present; transaction %d"
              , key, present ? "not " : "already", xact));
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////////////

  static Version findLargestWTS(int xact, List<Version> objects) {
    if (objects.isEmpty())
      return null;

    int writeIndex = -1, minW = Integer.MAX_VALUE;

    for (int i = 0; i < objects.size(); ++i) {
      int wts = objects.get(i).WTS;
      if (wts <= xact && minW > (xact - wts)) {
        minW = xact - wts;
        writeIndex = i;
      }
    }

    if (writeIndex != -1) {
      return objects.get(writeIndex);
    }
    return null;
  }

  // returns transaction id == logical start timestamp
  public static int begin_transaction() {
    ++max_xact;
    blockedByTransactions.put(max_xact, new HashSet<Integer>());
    selfBlockedTransactions.put(max_xact, new HashSet<Integer>());
    return max_xact;
  }

  // create and initialize new object in transaction xact
  public static void insert(int xact, int key, int value) throws Exception //insert == write
  {
    checkTransaction(xact);
    checkKey(key, xact, false);

    versions.put(key, new LinkedList<Version>(Arrays.asList(new Version(value, xact))));
  }

  // return value of object key in transaction xact
  public static int read(int xact, int key) throws Exception {
    checkTransaction(xact);
    checkKey(key, xact, true);

    Version objW = findLargestWTS(xact, versions.get(key));
    if (objW == null) {
      throw new Exception(String.format("empty database"));
    }

    objW.RTS = Math.max(xact, objW.RTS);

    // register dependency: obj.WTS transaction is started but has not yet committed
    if (isActive(objW.WTS)) {
      blockedByTransactions.get(xact).add(objW.WTS);
      if (objW.WTS != xact) {
        selfBlockedTransactions.get(objW.WTS).add(xact);
      }
    }

    return objW.value;
  }

  // write value of existing object identified by key in transaction xact
  public static void write(int xact, int key, int value) throws Exception {
    checkTransaction(xact);
    checkKey(key, xact, true);

    Version obj = findLargestWTS(xact, versions.get(key));

    int rts = obj.RTS, wts = obj.WTS;

    if (rts > xact) { //reject write
      rollback(xact);
      throw new Exception(String.format("TS(%d) < RTS(%d): write is rejected", xact, rts));
    }
    else {
      if (xact == wts) {
        obj.value = value;
      } 
	  else if (xact > wts) {
        versions.get(key).add(new Version(value, xact));
      }
    }
  }

  private static void commitCleanUP(int xact) {
    if (!selfBlockedTransactions.containsKey(xact)) {
      return;
    }

    for (Integer tr : selfBlockedTransactions.get(xact)) {
      blockedByTransactions.get(tr).remove(xact);
    }
    blockedByTransactions.remove(xact);
    selfBlockedTransactions.remove(xact);
  }


  public static void commit(int xact) throws Exception {

    checkTransaction(xact);

    Set<Integer> blockers = blockedByTransactions.get(xact);
    if (!blockers.isEmpty()) {
      pendingTransations.add(xact);
      return;
    }

    commitCleanUP(xact);

    // initiate commit of dependent pending transactions
    Iterator<Integer> it = pendingTransations.iterator();
    while (it.hasNext()) {
      Integer trTS = it.next();
      Set<Integer> blockedBy = blockedByTransactions.get(trTS);
      if (blockedBy != null && blockedBy.isEmpty()) {
        commit(trTS);
        it.remove();
      }
    }
  }

  public static void rollback(int xact) throws Exception {

    // remove all versions for aborted transaction
    for (Map.Entry<Integer, LinkedList<Version>> object : versions.entrySet()) {
      LinkedList<Version> res = object.getValue().stream()
              .filter(v -> v.WTS != xact)
              .collect(Collectors.toCollection(LinkedList::new));

      object.setValue(res);
    }

    blockedByTransactions.remove(xact);
    abortedTransactions.add(xact);

    // rollback dependencies
    for (Integer trId : selfBlockedTransactions.get(xact)) {
      try {
        if(pendingTransations.contains(trId)) {
          pendingTransations.remove(trId);
        }
        rollback(trId);
      }
      catch (Exception e) {}
    }
    selfBlockedTransactions.remove(xact);

    // Only throw the exception at the first rollback, catch the others (see above).
    throw new Exception(String.format("rollback of T %d", xact));
  }
}
