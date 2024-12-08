import java.rmi.Remote;
import java.rmi.RemoteException;

public interface SubscriberInterface extends Remote {
    void notify(String eventType, String imageName) throws RemoteException;
}
