/**
 *
 */
package no.hvl.dat110.chordoperations;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.middleware.Node;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Hash;
import no.hvl.dat110.util.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.Set;
import java.util.Timer;

/**
 * @author tdoy
 *
 */
public class ChordProtocols {

	private static final Logger logger = LogManager.getLogger(ChordProtocols.class);
	/**
	 * UpdateSuccessor
	 * StabilizeRing
	 * FixFingerTable
	 * CheckPredecessor
	 */

	private NodeInterface chordnode;
	private StabilizationProtocols stabprotocol;

    public ChordProtocols(NodeInterface chordnode) {
		this.chordnode = chordnode;
		joinRing();
		stabilizationProtocols();
	}

    /**
	 * Public access bcos it's used by the GUI
	 */
	public void stabilizationProtocols() {
		Timer timer = new Timer();
		stabprotocol = new StabilizationProtocols(this, timer);
		timer.scheduleAtFixedRate(stabprotocol, 5000, 2000);
	}

    /**
     * Public access bcos it's used by the GUI
     */
	public void joinRing() {

        try {
			Registry registry = Util.tryIPSingleMachine(chordnode.getNodeName());
			if(registry != null) {
				try {
                    String foundNode = Util.activeIP;

                    NodeInterface randomNode = (NodeInterface) registry.lookup(foundNode);

                    logger.info("JoinRing-randomNode = " + randomNode.getNodeName());

                    NodeInterface chordnodeSuccessor = randomNode.findSuccessor(chordnode.getNodeID());

                    chordnode.setSuccessor(chordnodeSuccessor);
                    chordnode.setPredecessor(null);

                    chordnodeSuccessor.notify(chordnode);

                    fixFingerTable();

                    ((Node) chordnode).copyKeysFromSuccessor(chordnodeSuccessor);

                    logger.info(chordnode.getNodeName() + " is between null | " + chordnodeSuccessor.getNodeName());

                } catch (Exception e) {
                    logger.error(e.getMessage());
                }
            } else {

                createRing((Node) chordnode);
            }
        } catch (NumberFormatException | RemoteException e1) {
            // TODO Auto-generated catch block
        }
    }

    private void createRing(Node node) throws RemoteException {

        node.setPredecessor(null);

        node.setSuccessor(node);

        logger.info("New ring created. Node = " + node.getNodeName() + " | Successor = " + node.getSuccessor().getNodeName() +
                " | Predecessor = " + node.getPredecessor());

    }

    public void leaveRing() throws RemoteException {

        logger.info("Attempting to update successor and predecessor before leaving the ring...");

        try {

            NodeInterface prednode = chordnode.getPredecessor();
            NodeInterface succnode = chordnode.getSuccessor();

            Set<BigInteger> keyids = chordnode.getNodeKeys();

            if (succnode != null) {
                keyids.forEach(fileID -> {
                    try {
                        logger.info("Adding fileID = " + fileID + " to " + succnode.getNodeName());

                        succnode.addKey(fileID);
                        Message msg = chordnode.getFilesMetadata().get(fileID);
                        succnode.saveFileContent(msg.getNameOfFile(), fileID, msg.getBytesOfFile(), msg.isPrimaryServer());
                    } catch (RemoteException e) {
                        //
                    }
                });

                succnode.setPredecessor(prednode);
            }
            if (prednode != null) {
                prednode.setSuccessor(succnode);
            }
            chordnode.setSuccessor(chordnode);
            chordnode.setPredecessor(chordnode);
            chordnode.getNodeKeys().clear();
            stabprotocol.setStop(true);

        } catch (Exception e) {
            //
            logger.error("some errors while updating succ/pred/keys...\n" + e.getMessage());
        }

        logger.info("Update of successor and predecessor completed...bye!");
    }

    public void fixFingerTable() {

        try {
            logger.info("Fixing the FingerTable for the Node: " + chordnode.getNodeName());

            List<NodeInterface> fingerTable = chordnode.getFingerTable();
            fingerTable.clear();
            BigInteger addressize = Hash.addressSize();
            int bitNumbers = Hash.bitSize();
            for (int i = 0; i < bitNumbers; i++) {
                BigInteger f = chordnode.getNodeID().add(BigInteger.valueOf(2).pow(i)).mod(addressize);
                NodeInterface succNode = chordnode.findSuccessor(f);
                if (succNode != null) {
                    fingerTable.add(succNode);
                }
            }

        } catch (RemoteException e) {
            logger.error("Fix fingerTable failed" + e.getMessage());
        }
    }

    protected NodeInterface getChordnode() {
        return chordnode;
    }

}