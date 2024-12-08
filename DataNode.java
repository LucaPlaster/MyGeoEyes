/*
    Sistemas Distribuídos

    LUCA MASCARENHAS PLASTER - 202014610
    MARCOS REGES MOTA - 202003598
*/

import java.io.File;
import java.io.FileOutputStream; 
import java.io.IOException;
import java.nio.file.Files;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

/**
 * Classe que representa um nó de dados (DataNode) responsável por armazenar partes de imagens.
 * Cada parte é salva como um arquivo no diretório local do DataNode.
 */
public class DataNode extends UnicastRemoteObject implements DataNodeInterface {
    private static final String STORAGE_DIR = "data_node_storage/";
    private String dataNodeId;

    /**
     * Construtor do DataNode.
     * @param dataNodeId Identificador único para este DataNode.
     * @throws RemoteException Em caso de falha de comunicação RMI.
     */
    protected DataNode(String dataNodeId) throws RemoteException {
        this.dataNodeId = dataNodeId;
        File dir = new File(STORAGE_DIR);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    @Override
    public boolean uploadPart(String imageName, int partNumber, byte[] data) throws RemoteException {
        try (FileOutputStream fos = new FileOutputStream(STORAGE_DIR + imageName + "_part" + partNumber)) {
            fos.write(data);
            System.out.println("DataNode " + dataNodeId + ": Parte " + partNumber + " da imagem '" + imageName + "' armazenada.");
            return true;
        } catch (IOException e) {
            System.err.println("DataNode " + dataNodeId + ": Erro ao armazenar a parte da imagem - " + e.getMessage());
            return false;
        }
    }

    @Override
    public byte[] downloadPart(String imageName, int partNumber) throws RemoteException {
        File file = new File(STORAGE_DIR + imageName + "_part" + partNumber);
        if (file.exists()) {
            try {
                byte[] data = Files.readAllBytes(file.toPath());
                System.out.println("DataNode " + dataNodeId + ": Parte " + partNumber + " da imagem '" + imageName + "' enviada.");
                return data;
            } catch (IOException e) {
                System.err.println("DataNode " + dataNodeId + ": Erro ao ler a parte da imagem - " + e.getMessage());
                return null;
            }
        } else {
            System.out.println("DataNode " + dataNodeId + ": Parte " + partNumber + " da imagem '" + imageName + "' não encontrada.");
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

    /**
     * Método para verificar se o DataNode está acessível. 
     * Retorna sempre true se o DataNode puder ser contactado via RMI.
     */
    @Override
    public boolean ping() throws RemoteException {
        return true;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Uso: java DataNode <DataNodeId>");
            System.exit(1);
        }

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
            e.printStackTrace();
        }
    }
}
