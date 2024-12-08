/*
    Sistemas Distribuídos

    LUCA MASCARENHAS PLASTER - 202014610
    MARCOS REGES MOTA - 202003598
*/

import java.rmi.Remote;
import java.rmi.RemoteException;

public interface DataNodeInterface extends Remote {
    boolean uploadPart(String imageName, int partNumber, byte[] data) throws RemoteException;
    byte[] downloadPart(String imageName, int partNumber) throws RemoteException;
    boolean deletePart(String imageName, int partNumber) throws RemoteException;

    boolean ping() throws RemoteException;
}
