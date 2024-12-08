/*
    Sistemas Distribuídos
    
    LUCA MASCARENHAS PLASTER - 202014610
    MARCOS REGES MOTA - 202003598
*/


import java.io.File;
import java.io.FileOutputStream; 
import java.io.FileInputStream;  
import java.io.IOException;
import java.nio.file.Files;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.HashMap;
import java.util.Map;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class DataNode extends UnicastRemoteObject implements DataNodeInterface {
    private static final String STORAGE_DIR = "data_node_storage/";
    private String dataNodeId;

    protected DataNode(String dataNodeId) throws RemoteException {
        this.dataNodeId = dataNodeId;
        File dir = new File(STORAGE_DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    @Override
    public boolean uploadPart(String imageName, int partNumber, byte[] data) throws RemoteException {
        try {
            FileOutputStream fos = new FileOutputStream(STORAGE_DIR + imageName + "_part" + partNumber);
            fos.write(data);
            fos.close();
            System.out.println("DataNode " + dataNodeId + ": Parte " + partNumber + " da imagem '" + imageName + "' armazenada.");
            return true;
        } catch (IOException e) {
            System.err.println("Erro ao armazenar a parte da imagem: " + e.getMessage());
            return false;
        }
    }

    @Override
    public byte[] downloadPart(String imageName, int partNumber) throws RemoteException {
        try {
            File file = new File(STORAGE_DIR + imageName + "_part" + partNumber);
            if (file.exists()) {
                byte[] data = Files.readAllBytes(file.toPath());
                System.out.println("DataNode " + dataNodeId + ": Parte " + partNumber + " da imagem '" + imageName + "' enviada.");
                return data;
            } else {
                System.out.println("DataNode " + dataNodeId + ": Parte " + partNumber + " da imagem '" + imageName + "' não encontrada.");
                return null;
            }
        } catch (IOException e) {
            System.err.println("Erro ao ler a parte da imagem: " + e.getMessage());
            return null;
        }
    }

    @Override
    public boolean deletePart(String imageName, int partNumber) throws RemoteException {
        File file = new File(STORAGE_DIR + imageName + "_part" + partNumber);
        if (file.exists() && file.delete()) {
            System.out.println("DataNode " + dataNodeId + ": Parte " + partNumber + " da imagem '" + imageName + "' deletada.");
            return true;
        } else {
            System.out.println("DataNode " + dataNodeId + ": Falha ao deletar a parte " + partNumber + " da imagem '" + imageName + "'.");
            return false;
        }
    }

    public static void main(String[] args) {
        try {
            String dataNodeId = args[0];
            DataNode dataNode = new DataNode(dataNodeId);
            Registry registry = LocateRegistry.getRegistry();
            registry.rebind("DataNode_" + dataNodeId, dataNode);
            System.out.println("DataNode " + dataNodeId + " registrado no RMI Registry.");

            // Registrar no MasterServer
            MasterServerInterface master = (MasterServerInterface) registry.lookup("MasterServer");
            master.registerDataNode(dataNodeId, dataNode);
            System.out.println("DataNode " + dataNodeId + " registrado no MasterServer.");

        } catch (Exception e) {
            System.err.println("Erro no DataNode: " + e.getMessage());
        }
    }
}
