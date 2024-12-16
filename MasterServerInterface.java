/*
    Sistemas Distribuídos
    
    LUCA MASCARENHAS PLASTER - 202014610
    MARCOS REGES MOTA - 202003598
*/

import java.rmi.Remote;
import java.rmi.RemoteException;
import java.util.List;
import java.util.Map;

public interface MasterServerInterface extends Remote {
    void registerDataNode(String dataNodeId, DataNodeInterface dataNode) throws RemoteException;
    void unregisterDataNode(String dataNodeId) throws RemoteException;
    List<String> listImages() throws RemoteException;
    Map<Integer, DataNodeInterface> getImageParts(String imageName) throws RemoteException;
    boolean storeImage(String imageName, byte[] imageData, int numParts) throws RemoteException;
    boolean deleteImage(String imageName) throws RemoteException;
    public void handleDataNodeFailure(String dataNodeId) throws RemoteException;
    

    // -------------------------
    // Métodos para Pub/Sub
    // -------------------------

    void subscribe(String eventType, SubscriberInterface subscriberInterface) throws RemoteException;
    void unsubscribe(String eventType, SubscriberInterface subscriberInterface) throws RemoteException;
    List<String> listEventTypes() throws RemoteException;
}
