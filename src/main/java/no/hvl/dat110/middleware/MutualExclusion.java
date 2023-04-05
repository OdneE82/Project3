/**
 *
 */
package no.hvl.dat110.middleware;

import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.LamportClock;
import no.hvl.dat110.util.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author tdoy
 */
public class MutualExclusion {

    private static final Logger logger = LogManager.getLogger(MutualExclusion.class);
    /**
     * lock variables
     */
    private boolean CS_BUSY = false;
    private boolean WANTS_TO_ENTER_CS = false;
    private List<Message> queueack;
    private List<Message> mutexqueue;

    private LamportClock clock;
    private Node node;

    public MutualExclusion(Node node) throws RemoteException {
        this.node = node;

        clock = new LamportClock();
        queueack = new ArrayList<Message>();
        mutexqueue = new ArrayList<Message>();
    }

    public synchronized void acquireLock() {
        CS_BUSY = true;
    }

    public void releaseLocks() {
        WANTS_TO_ENTER_CS = false;
        CS_BUSY = false;
    }

	public boolean doMutexRequest(Message message, byte[] updates) throws RemoteException {
        logger.info(node.nodename + " wants to access CS");

        queueack.clear();
        mutexqueue.clear();

        clock.increment();

        message.setClock(clock.getClock());

        WANTS_TO_ENTER_CS = true;

        List<Message> messages = removeDuplicatePeersBeforeVoting();

        multicastMessage(message, messages);

        if (areAllMessagesReturned(messages.size())) {
            acquireLock();

            node.broadcastUpdatetoPeers(updates);

            mutexqueue.clear();

            return true;
        }

        return false;
    }

    private void multicastMessage(Message message, List<Message> activenodes) throws RemoteException {

        logger.info("Number of peers to vote = " + activenodes.size());

        for (Message msg : activenodes) {
            NodeInterface stub = Util.getProcessStub(msg.getNodeName(), msg.getPort());
            if (stub != null) {
                stub.onMutexRequestReceived(message);
            }
        }

    }

    public void onMutexRequestReceived(Message message) throws RemoteException {

        // if message is from self, acknowledge, and call onMutexAcknowledgementReceived()
        if (message.getNodeName().equals(node.nodename)) {
            message.setAcknowledged(true);
            onMutexAcknowledgementReceived(message);
        }

        int caseid = (!CS_BUSY && !WANTS_TO_ENTER_CS) ? 0 : CS_BUSY ? 1 : 2;

        doDecisionAlgorithm(message, mutexqueue, caseid);
    }

    public void doDecisionAlgorithm(Message message, List<Message> queue, int condition) throws RemoteException {

        String procName = message.getNodeName();
        int port = message.getPort();

        switch (condition) {

            /** case 1: Receiver is not accessing shared resource and does not want to (send OK to sender) */
            case 0: {


                NodeInterface stub = Util.getProcessStub(procName, port);
                message.setAcknowledged(true);
                stub.onMutexAcknowledgementReceived(message);
                break;
            }

            /** case 2: Receiver already has access to the resource (dont reply but queue the request) */
            case 1: {
                queue.add(message);
                // queue this message
                break;
            }

            /**
             *  case 3: Receiver wants to access resource but is yet to (compare own message clock to received message's clock
             *  the message with lower timestamp wins) - send OK if received is lower. Queue message if received is higher
             */
            case 2: {

                int oClock = clock.getClock();
                int sClock = message.getClock();

                if (oClock == sClock)
                    if (message.getNodeID().compareTo(node.getNodeID()) > 0)
                        queue.add(message); // Sender loses
                    else { // Sender wins
                        NodeInterface stub = Util.getProcessStub(procName, port);
                        stub.onMutexAcknowledgementReceived(message);
                        message.setAcknowledged(true);

                    }
                else if (oClock > sClock) {
                    message.setAcknowledged(true);
                    NodeInterface stub = Util.getProcessStub(procName, port);
                    stub.onMutexAcknowledgementReceived(message);

                } else
                    queue.add(message);

                break;
            }

            default:
                break;
        }

    }

    public void onMutexAcknowledgementReceived(Message message) throws RemoteException {

        queueack.add(message);
    }

    public void multicastReleaseLocks(Set<Message> activenodes) {
        logger.info("Releasing locks from = " + activenodes.size());


        for (Message msg : activenodes) {
            NodeInterface stub = Util.getProcessStub(msg.getNodeName(), msg.getPort());
            try {
                stub.releaseLocks();
            } catch (RemoteException e) {
                e.printStackTrace();
            }

        }
        releaseLocks();
    }

    private boolean areAllMessagesReturned(int numvoters) throws RemoteException {
        logger.info(node.getNodeName() + ": size of queueack = " + queueack.size());


        if (queueack.size() == numvoters) {
            queueack.clear();
            return true;
        }

        return false;
    }

    private List<Message> removeDuplicatePeersBeforeVoting() {

        List<Message> uniquepeer = new ArrayList<Message>();
        for (Message p : node.activenodesforfile) {
            boolean found = false;
            for (Message p1 : uniquepeer) {
                if (p.getNodeName().equals(p1.getNodeName())) {
                    found = true;
                    break;
                }
            }
            if (!found)
                uniquepeer.add(p);
        }
        return uniquepeer;
	}
}