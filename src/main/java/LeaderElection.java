import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {
    private static final String ZOOKEEPER_ADDRESS = "localhost:2181";
    private static final String ELECTION_NAMESPACE = "/election";
    private ZooKeeper zooKeeper;
    private static final int SESSION_TIMEOUT = 5000;
    private String currentZnodeName;

    public static void main(String args[]) throws IOException, InterruptedException, KeeperException {
        LeaderElection leaderElection = new LeaderElection();

        leaderElection.connectToZooKeeper();
        leaderElection.createParentNodeIfNotExist(ELECTION_NAMESPACE);
        leaderElection.volunteerForLeadership();
        leaderElection.reElectLeader();
        leaderElection.run();
        leaderElection.close();
        System.out.println("Disconnected from ZK. Exiting...");
    }

    public void createParentNodeIfNotExist(String parent) throws KeeperException, InterruptedException {
        Stat stat = zooKeeper.exists(parent, false);
        if (stat == null)
        {
            String node = zooKeeper.create(parent, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            System.out.println(node + " base path created");
            return;
        }
        System.out.println(parent + " already exists.");
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String znodePrefix = ELECTION_NAMESPACE + "/c_";
        String zndoeFullPath = zooKeeper.create(znodePrefix, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

        System.out.println("znode name: "+zndoeFullPath);
        this.currentZnodeName = zndoeFullPath.replace(ELECTION_NAMESPACE+"/", "");
    }

    public void reElectLeader() throws KeeperException, InterruptedException {
        String predecessorNode = "";
        Stat predecessorNodeStat = null;

        while (predecessorNodeStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);
            Collections.sort(children);
            String smallestNode = children.get(0);
            if (smallestNode.equals(currentZnodeName)) {
                System.out.println("I am the leader: " + currentZnodeName);
                return;
            } else {
                System.out.println(smallestNode + " is the leader");
                int prevNodeIndex = Collections.binarySearch(children, currentZnodeName) - 1;
                predecessorNode = children.get(prevNodeIndex);
                predecessorNodeStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorNode, this);
            }
        }
        System.out.println("Watching my predecessor : " + predecessorNode);
    }

    public void connectToZooKeeper() throws IOException {
        this.zooKeeper = new ZooKeeper(ZOOKEEPER_ADDRESS, SESSION_TIMEOUT, this);
    }

    public void run() throws InterruptedException
    {
        synchronized (zooKeeper){
            zooKeeper.wait();
        }
    }

    public void close() throws InterruptedException {
        zooKeeper.close();
    }

    @Override
    public void process(WatchedEvent watchedEvent) {
        switch (watchedEvent.getType()) {
            case None:
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println("Connected to ZK");
                } else {
                    synchronized (zooKeeper) {
                        System.out.println("Disconnected event received from ZK");
                        zooKeeper.notifyAll();
                    }
                }
                break;
            case NodeDeleted:
                try {
                    System.out.println("Re-electing the leader");
                    reElectLeader();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }
}
