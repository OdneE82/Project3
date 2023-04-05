/**
 *
 */
package no.hvl.dat110.chordoperations;

import no.hvl.dat110.middleware.Message;
import no.hvl.dat110.middleware.Node;
import no.hvl.dat110.rpc.interfaces.NodeInterface;
import no.hvl.dat110.util.Util;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigInteger;
import java.rmi.RemoteException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author tdoy
 *
 */
public class ChordLookup {

	private static final Logger logger = LogManager.getLogger(ChordLookup.class);
	private Node node;

	public ChordLookup(Node node) {
		this.node = node;
	}

	public NodeInterface findSuccessor(BigInteger key) throws RemoteException {

		NodeInterface succ = node.getSuccessor();
		NodeInterface stub = Util.getProcessStub(succ.getNodeName(), succ.getPort());
		if (stub != null && Util.checkInterval(key, node.getNodeID().add(new BigInteger("1")), stub.getNodeID())) {
			return stub;
		}
		return findHighestPredecessor(key).findSuccessor(key);
	}

	/**
	 * This method makes a remote call. Invoked from a local client
	 * @param ID BigInteger
	 * @return
	 * @throws RemoteException
	 */
	private NodeInterface findHighestPredecessor(BigInteger ID) throws RemoteException {

		List<NodeInterface> fingerTable = node.getFingerTable();

		for (int i = fingerTable.size() - 1; i >= 0; i--) {
			NodeInterface finger = fingerTable.get(i);
			NodeInterface stub = Util.getProcessStub(finger.getNodeName(), finger.getPort());

			assert stub != null;
			if (Util.checkInterval(finger.getNodeID(), node.getNodeID().add(new BigInteger("1")), ID.subtract(new BigInteger("1")))) {
				return stub;
			}
		}

		return node;
	}

	public void copyKeysFromSuccessor(NodeInterface succ) {

		Set<BigInteger> filekeys;
		try {
			if(succ.getNodeName().equals(node.getNodeName()))
				return;

			logger.info("copy file keys that are <= "+node.getNodeName()+" from successor "+ succ.getNodeName()+" to "+node.getNodeName());

			filekeys = new HashSet<>(succ.getNodeKeys());
			BigInteger nodeID = node.getNodeID();

			for(BigInteger fileID : filekeys) {

				if(fileID.compareTo(nodeID) <= 0) {
					logger.info("fileID=" + fileID + " | nodeID= " + nodeID);
					node.addKey(fileID);
					Message msg = succ.getFilesMetadata().get(fileID);
					node.saveFileContent(msg.getNameOfFile(), fileID, msg.getBytesOfFile(), msg.isPrimaryServer());
					succ.removeKey(fileID);
					succ.getFilesMetadata().remove(fileID);
				}
			}

			logger.info("Finished copying file keys from successor "+ succ.getNodeName()+" to "+node.getNodeName());
		} catch (RemoteException e) {
			logger.error(e.getMessage());
		}
	}

	public void notify(NodeInterface pred_new) throws RemoteException {

		NodeInterface pred_old = node.getPredecessor();

		if (pred_old == null) {
			node.setPredecessor(pred_new);
			return;
		} else if (pred_new.getNodeName().equals(node.getNodeName())) {
			node.setPredecessor(null);
			return;
		} else {
			BigInteger nodeID = node.getNodeID();
			BigInteger pred_oldID = pred_old.getNodeID();

			BigInteger pred_newID = pred_new.getNodeID();

			boolean cond = Util.checkInterval(pred_newID, pred_oldID.add(BigInteger.ONE), nodeID.add(BigInteger.ONE));
			if (cond) {
				node.setPredecessor(pred_new);
			}
		}
	}

}