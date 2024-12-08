/*
    Sistemas Distribuídos
    
    LUCA MASCARENHAS PLASTER - 202014610
    MARCOS REGES MOTA - 202003598
*/

import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;

public class MasterServer extends UnicastRemoteObject implements MasterServerInterface {
    private Map<String, DataNodeInterface> dataNodes = Collections.synchronizedMap(new HashMap<>());
    private Map<String, Map<Integer, String>> imageParts = Collections.synchronizedMap(new HashMap<>());
    private int replicationFactor;

    protected MasterServer(int replicationFactor) throws RemoteException {
        this.replicationFactor = replicationFactor;
    }

    @Override
    public void registerDataNode(String dataNodeId, DataNodeInterface dataNode) throws RemoteException {
        dataNodes.put(dataNodeId, dataNode);
        System.out.println("DataNode " + dataNodeId + " registrado.");
    }

    @Override
    public List<String> listImages() throws RemoteException {
        return new ArrayList<>(imageParts.keySet());
    }

    @Override
    public Map<Integer, DataNodeInterface> getImageParts(String imageName) throws RemoteException {
        Map<Integer, DataNodeInterface> partsMap = new HashMap<>();
        Map<Integer, String> parts = imageParts.get(imageName);
        if (parts != null) {
            for (Map.Entry<Integer, String> entry : parts.entrySet()) {
                partsMap.put(entry.getKey(), dataNodes.get(entry.getValue()));
            }
            return partsMap;
        } else {
            return null;
        }
    }

    @Override
    public boolean storeImage(String imageName, byte[] imageData, int numParts) throws RemoteException {
        try {
            // Dividir a imagem em partes
            int partSize = imageData.length / numParts;
            Map<Integer, String> partsMap = new HashMap<>();
            List<String> dataNodeIds = new ArrayList<>(dataNodes.keySet());
            Collections.shuffle(dataNodeIds);
            int dataNodeIndex = 0;

            for (int i = 0; i < numParts; i++) {
                int start = i * partSize;
                int end = (i == numParts - 1) ? imageData.length : start + partSize;
                byte[] partData = Arrays.copyOfRange(imageData, start, end);

                // Seleciona um DataNode para armazenar a parte
                String dataNodeId = dataNodeIds.get(dataNodeIndex % dataNodeIds.size());
                DataNodeInterface dataNode = dataNodes.get(dataNodeId);

                if (dataNode.uploadPart(imageName, i, partData)) {
                    partsMap.put(i, dataNodeId);
                } else {
                    System.err.println("Falha ao armazenar a parte " + i + " da imagem '" + imageName + "'.");
                    return false;
                }

                dataNodeIndex++;
            }

            imageParts.put(imageName, partsMap);
            System.out.println("Imagem '" + imageName + "' armazenada com sucesso.");
            return true;
        } catch (Exception e) {
            System.err.println("Erro ao armazenar a imagem: " + e.getMessage());
            return false;
        }
    }

    @Override
    public boolean deleteImage(String imageName) throws RemoteException {
        Map<Integer, String> parts = imageParts.remove(imageName);
        if (parts != null) {
            for (Map.Entry<Integer, String> entry : parts.entrySet()) {
                DataNodeInterface dataNode = dataNodes.get(entry.getValue());
                dataNode.deletePart(imageName, entry.getKey());
            }
            System.out.println("Imagem '" + imageName + "' deletada com sucesso.");
            return true;
        } else {
            System.out.println("Imagem '" + imageName + "' não encontrada.");
            return false;
        }
    }

    public static void main(String[] args) {
        try {
            int replicationFactor = 1;
            if (args.length > 0) {
                replicationFactor = Integer.parseInt(args[0]);
            }
            MasterServer masterServer = new MasterServer(replicationFactor);
            Registry registry = LocateRegistry.createRegistry(1099);
            registry.rebind("MasterServer", masterServer);
            System.out.println("MasterServer iniciado e registrado no RMI Registry.");
        } catch (Exception e) {
            System.err.println("Erro no MasterServer: " + e.getMessage());
        }
    }
}
