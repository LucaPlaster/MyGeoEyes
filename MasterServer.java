import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Classe que implementa o servidor mestre responsável por gerenciar o mapeamento de imagens e seus DataNodes,
 * bem como oferecer mecanismos de pub/sub para eventos de adição e remoção de imagens.
 */
public class MasterServer extends UnicastRemoteObject implements MasterServerInterface {
    private Map<String, DataNodeInterface> dataNodes = Collections.synchronizedMap(new HashMap<>());
    /**
     * Mapa que armazena as partes da imagem:
     * chave: nome da imagem
     * valor: Mapa (parte -> dataNodeId)
     */
    private Map<String, Map<Integer, String>> imageParts = Collections.synchronizedMap(new HashMap<>());

    private int replicationFactor;
    private MonitorServiceInterface monitorService;

    // Mapa de listas de assinantes por tipo de evento
    private Map<String, List<SubscriberInterface>> subscribersByEventType = Collections.synchronizedMap(new HashMap<>());

    // Lista de tipos de eventos disponíveis
    private static final List<String> EVENT_TYPES = Arrays.asList("IMAGE_ADDED", "IMAGE_DELETED");

    /**
     * Construtor do MasterServer.
     * @param replicationFactor fator de replicação (não totalmente implementado neste exemplo)
     * @throws RemoteException em caso de falha de comunicação RMI
     */
    protected MasterServer(int replicationFactor) throws RemoteException {
        this.replicationFactor = replicationFactor;
        // Inicializa o mapa de assinantes com listas vazias
        for (String eventType : EVENT_TYPES) {
            subscribersByEventType.put(eventType, Collections.synchronizedList(new ArrayList<>()));
        }

        // Conecta-se ao MonitorService
        try {
            Registry monitorRegistry = LocateRegistry.getRegistry("localhost", 2000);
            monitorService = (MonitorServiceInterface) monitorRegistry.lookup("MonitorService");
            monitorService.registerMasterServer(this);
            System.out.println("MasterServer registrado no MonitorService.");
        } catch (Exception e) {
            System.err.println("Erro ao conectar com o MonitorService: " + e.getMessage());
        }
    }

    @Override
    public void registerDataNode(String dataNodeId, DataNodeInterface dataNode) throws RemoteException {
    Registry registry = LocateRegistry.getRegistry("localhost", 1098); 
    registry.rebind("DataNode_" + dataNodeId, dataNode);
    System.out.println("Datanode " + dataNodeId + " registrado com sucesso");

    // Adicionar o DataNode ao mapa dataNodes
    dataNodes.put(dataNodeId, dataNode);
    }


    /**
     * Caso queira remover DataNodes manualmente, este método pode estar na interface.  
     * Se não estiver na interface atualizada, remova ou adicione conforme necessidade.
     * Aqui estou assumindo que a interface foi atualizada para suportar este método.
     */
    @Override
    public void unregisterDataNode(String dataNodeId) throws RemoteException {
        dataNodes.remove(dataNodeId);
        System.out.println("DataNode " + dataNodeId + " removido do registro.");
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
                String dataNodeId = entry.getValue();
                DataNodeInterface dataNode = dataNodes.get(dataNodeId);
                if (dataNode == null) {
                    System.err.println("DataNode " + dataNodeId + " não encontrado. Notificando o MonitorService.");
                    notifyMonitorService(dataNodeId);
                    return null;
                }
                try {
                    // Verifica se o DataNode está acessível
                    if (dataNode.ping()) {
                        partsMap.put(entry.getKey(), dataNode);
                    } else {
                        System.err.println("DataNode " + dataNodeId + " inacessível. Notificando o MonitorService.");
                        notifyMonitorService(dataNodeId);
                        return null;
                    }
                } catch (RemoteException e) {
                    System.err.println("Falha ao contatar DataNode " + dataNodeId + ". Notificando o MonitorService.");
                    notifyMonitorService(dataNodeId);
                    return null;
                }
            }
            return partsMap;
        } else {
            return null;
        }
    }

    @Override
    public boolean storeImage(String imageName, byte[] imageData, int numParts) throws RemoteException {
        try {
            int partSize = imageData.length / numParts;
            Map<Integer, String> partsMap = new HashMap<>();
            List<String> dataNodeIds = new ArrayList<>(dataNodes.keySet());
            if (dataNodeIds.isEmpty()) {
                System.err.println("Nenhum DataNode disponível para armazenar a imagem.");
                return false;
            }
            Collections.shuffle(dataNodeIds);
            int dataNodeIndex = 0;
            for (int i = 0; i < numParts; i++) {
                int start = i * partSize;
                int end = (i == numParts - 1) ? imageData.length : start + partSize;
                byte[] partData = Arrays.copyOfRange(imageData, start, end);
                String dataNodeId = dataNodeIds.get(dataNodeIndex % dataNodeIds.size());
                DataNodeInterface dataNode = dataNodes.get(dataNodeId);
                if (dataNode == null) {
                    System.err.println("DataNode " + dataNodeId + " não encontrado durante o upload. Notificando o MonitorService.");
                    notifyMonitorService(dataNodeId);
                    return false;
                }
                try {
                    if (dataNode.uploadPart(imageName, i, partData)) {
                        partsMap.put(i, dataNodeId);
                    } else {
                        System.err.println("Falha ao armazenar a parte " + i + " da imagem '" + imageName + "'.");
                        return false;
                    }
                } catch (RemoteException e) {
                    System.err.println("DataNode " + dataNodeId + " inacessível durante o upload. Notificando o MonitorService.");
                    notifyMonitorService(dataNodeId);
                    return false;
                }
                dataNodeIndex++;
            }

            imageParts.put(imageName, partsMap);
            System.out.println("Imagem '" + imageName + "' armazenada com sucesso.");

            // Notifica assinantes do evento IMAGE_ADDED
            notifySubscribers("IMAGE_ADDED", imageName);

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
                String dataNodeId = entry.getValue();
                DataNodeInterface dataNode = dataNodes.get(dataNodeId);
                if (dataNode == null) {
                    System.err.println("DataNode " + dataNodeId + " não encontrado durante a exclusão. Notificando o MonitorService.");
                    notifyMonitorService(dataNodeId);
                    continue;
                }
                try {
                    dataNode.deletePart(imageName, entry.getKey());
                } catch (RemoteException e) {
                    System.err.println("DataNode " + dataNodeId + " inacessível durante a exclusão. Notificando o MonitorService.");
                    notifyMonitorService(dataNodeId);
                }
            }

            System.out.println("Imagem '" + imageName + "' deletada com sucesso.");

            // Notifica assinantes do evento IMAGE_DELETED
            notifySubscribers("IMAGE_DELETED", imageName);

            return true;
        } else {
            System.out.println("Imagem '" + imageName + "' não encontrada.");
            return false;
        }
    }

    // Métodos Pub/Sub implementados conforme a interface atualizada

    @Override
    public void subscribe(String eventType, SubscriberInterface subscriber) throws RemoteException {
        if (!EVENT_TYPES.contains(eventType)) {
            System.err.println("Tipo de evento inválido: " + eventType);
            return;
        }
        List<SubscriberInterface> subs = subscribersByEventType.get(eventType);
        synchronized (subs) {
            if (!subs.contains(subscriber)) {
                subs.add(subscriber);
                System.out.println("Novo assinante adicionado para o evento: " + eventType);
            }
        }
    }

    @Override
    public void unsubscribe(String eventType, SubscriberInterface subscriber) throws RemoteException {
        if (!EVENT_TYPES.contains(eventType)) {
            System.err.println("Tipo de evento inválido: " + eventType);
            return;
        }
        List<SubscriberInterface> subs = subscribersByEventType.get(eventType);
        synchronized (subs) {
            subs.remove(subscriber);
            System.out.println("Assinante removido do evento: " + eventType);
        }
    }

    @Override
    public List<String> listEventTypes() throws RemoteException {
        return new ArrayList<>(EVENT_TYPES);
    }

    /**
     * Notifica o MonitorService de falha em um DataNode.
     * @param dataNodeId ID do DataNode com falha
     */
    private void notifyMonitorService(String dataNodeId) {
        try {
            if (monitorService != null) {
                monitorService.notifyFailure(dataNodeId);
            } else {
                System.err.println("MonitorService não está disponível para notificação de falhas.");
            }
        } catch (Exception e) {
            System.err.println("Erro ao notificar o MonitorService: " + e.getMessage());
        }
    }

    /**
     * Notifica todos os assinantes de um determinado evento.
     * @param eventType Tipo do evento (ex: IMAGE_ADDED, IMAGE_DELETED)
     * @param imageName Nome da imagem associada ao evento
     */
    private void notifySubscribers(String eventType, String imageName) {
        List<SubscriberInterface> subs = subscribersByEventType.get(eventType);
        if (subs == null) return;
        synchronized (subs) {
            for (SubscriberInterface subscriber : subs) {
                try {
                    subscriber.notify(eventType, imageName);
                } catch (RemoteException e) {
                    System.err.println("Falha ao notificar um assinante do evento " + eventType + ": " + e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            int replicationFactor = 1;
            if (args.length > 0) {
                replicationFactor = Integer.parseInt(args[0]);
            }
            MasterServer masterServer = new MasterServer(replicationFactor);
            Registry registry = LocateRegistry.getRegistry("localhost", 1098);
            registry.rebind("MasterServer", masterServer);

            System.out.println("MasterServer iniciado e registrado no RMI Registry.");
        } catch (Exception e) {
            System.err.println("Erro no MasterServer: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
