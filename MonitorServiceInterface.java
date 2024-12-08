import java.rmi.Remote;
import java.rmi.RemoteException;

public interface MonitorServiceInterface extends Remote {
    void registerMasterServer(MasterServerInterface master) throws RemoteException;
    void notifyFailure(String dataNodeId) throws RemoteException;
}
