import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.*;

/**
 * Classe que implementa o servidor mestre responsável por gerenciar o
 * mapeamento de imagens e seus DataNodes,
 * bem como oferecer mecanismos de pub/sub para eventos de adição e remoção de
 * imagens.
 */
public class MasterServer extends UnicastRemoteObject implements MasterServerInterface {
    private Map<String, DataNodeInterface> dataNodes = Collections.synchronizedMap(new HashMap<>());
    /**
     * Mapa que armazena as partes da imagem:
     * chave: nome da imagem
     * valor: Mapa (parte -> dataNodeId)
     */
    private Map<String, Map<Integer, List<String>>> imageParts = Collections.synchronizedMap(new HashMap<>());

    private int replicationFactor;
    private MonitorServiceInterface monitorService;

    // Mapa de listas de assinantes por tipo de evento
    private Map<String, List<SubscriberInterface>> subscribersByEventType = Collections
            .synchronizedMap(new HashMap<>());

    // Lista de tipos de eventos disponíveis
    private static final List<String> EVENT_TYPES = Arrays.asList("IMAGE_ADDED", "IMAGE_DELETED");

    /**
     * Construtor do MasterServer.
     * 
     * @param replicationFactor fator de replicação (não totalmente implementado
     *                          neste exemplo)
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
        startDataNodeHeartbeatCheck();
    }

    private void startDataNodeHeartbeatCheck() {
        Thread monitorThread = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    checkDataNodeHealth();
                } catch (InterruptedException e) {
                }
            }
        });
        monitorThread.setDaemon(true);
        monitorThread.start();
    }

    private void checkDataNodeHealth() {
        List<String> nodesToRemove = new ArrayList<>();
        for (Map.Entry<String, DataNodeInterface> entry : dataNodes.entrySet()) {
            String nodeId = entry.getKey();
            try {
                if (!entry.getValue().ping()) {
                    System.err.println("DataNode " + nodeId + " inacessível. Notificando MonitorService.");
                    nodesToRemove.add(nodeId);
                }
            } catch (RemoteException e) {
                System.err.println("DataNode " + nodeId + " inacessível (exceção). Notificando MonitorService.");
                nodesToRemove.add(nodeId);
            }
        }

        for (String nodeId : nodesToRemove) {
            // Notifica o MonitorService
            notifyMonitorService(nodeId);
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
     * Caso queira remover DataNodes manualmente, este método pode estar na
     * interface.
     * Se não estiver na interface atualizada, remova ou adicione conforme
     * necessidade.
     * Aqui estou assumindo que a interface foi atualizada para suportar este
     * método.
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
        Map<Integer, DataNodeInterface> partsMapResult = new HashMap<>();
        Map<Integer, List<String>> parts = imageParts.get(imageName);
        if (parts != null) {
            for (Map.Entry<Integer, List<String>> entry : parts.entrySet()) {
                int partNumber = entry.getKey();
                List<String> replicas = entry.getValue();

                DataNodeInterface chosenDataNode = null;

                // Tentar achar um DataNode acessível entre as réplicas
                for (String dataNodeId : replicas) {
                    DataNodeInterface dataNode = dataNodes.get(dataNodeId);
                    if (dataNode == null) {
                        System.err.println("DataNode " + dataNodeId + " não encontrado. Notificando o MonitorService.");
                        notifyMonitorService(dataNodeId);
                        // Tenta o próximo DataNode
                        continue;
                    }
                    try {
                        // Verifica se o DataNode está acessível
                        if (dataNode.ping()) {
                            chosenDataNode = dataNode;
                            break; // Achou um DataNode acessível, pode parar de procurar
                        } else {
                            System.err
                                    .println("DataNode " + dataNodeId + " inacessível. Notificando o MonitorService.");
                            notifyMonitorService(dataNodeId);
                        }
                    } catch (RemoteException e) {
                        System.err.println(
                                "Falha ao contatar DataNode " + dataNodeId + ". Notificando o MonitorService.");
                        notifyMonitorService(dataNodeId);
                    }
                }

                if (chosenDataNode == null) {
                    // Não encontramos nenhum DataNode acessível para esta parte
                    System.err.println("Nenhuma réplica acessível da parte " + partNumber + " da imagem " + imageName
                            + " encontrada.");
                    return null;
                }

                partsMapResult.put(partNumber, chosenDataNode);
            }
            return partsMapResult;
        } else {
            return null;
        }
    }

    private void redistributeDataFromFailedNode(String failedNodeId) {
        System.out.println("Redistribuindo dados do DataNode falho: " + failedNodeId);
    
        // Percorrer todas as imagens e suas partes
        for (Map.Entry<String, Map<Integer, List<String>>> imageEntry : imageParts.entrySet()) {
            String imageName = imageEntry.getKey();
            Map<Integer, List<String>> partsMap = imageEntry.getValue();
    
            for (Map.Entry<Integer, List<String>> partEntry : partsMap.entrySet()) {
                int partNumber = partEntry.getKey();
                List<String> replicas = partEntry.getValue();
    
                // Remove o nó falho da lista de réplicas, se presente
                boolean removed = replicas.remove(failedNodeId);
                if (!removed) {
                    // Essa parte não estava armazenada no nó falho, então não precisa relocar
                    continue;
                }
    
                // Verifica se a parte ainda possui réplicas suficientes
                if (replicas.size() >= replicationFactor) {
                    // Já tem réplicas suficientes, não precisa fazer nada
                    continue;
                }
    
                // Tentar recriar réplicas para atingir o replicationFactor
                int replicasNeeded = replicationFactor - replicas.size();
                
                // Se não temos nenhuma réplica, a parte está irrecuperável (sem backup)
                if (replicas.isEmpty()) {
                    System.err.println("A parte " + partNumber + " da imagem " + imageName 
                                       + " foi perdida, pois todas as réplicas estavam no DataNode falho " + failedNodeId);
                    // Poderia marcar a imagem como corrompida, ou apenas seguir
                    continue;
                }
    
                // Temos pelo menos uma réplica, pegar a primeira como fonte para copiar a parte
                String sourceNodeId = replicas.get(0);
                DataNodeInterface sourceNode = dataNodes.get(sourceNodeId);
                if (sourceNode == null) {
                    System.err.println("Não foi possível acessar o DataNode " + sourceNodeId 
                                       + " para replicar a parte " + partNumber + " da imagem " + imageName);
                    continue;
                }
    
                // Baixar a parte do DataNode fonte
                byte[] partData = null;
                try {
                    partData = sourceNode.downloadPart(imageName, partNumber);
                } catch (RemoteException e) {
                    System.err.println("Falha ao baixar a parte " + partNumber + " da imagem " + imageName 
                                       + " do DataNode " + sourceNodeId + ": " + e.getMessage());
                    continue;
                }
    
                if (partData == null) {
                    System.err.println("A parte " + partNumber + " da imagem " + imageName 
                                       + " não pode ser recuperada do DataNode " + sourceNodeId);
                    continue;
                }
    
                // Tentar encontrar DataNodes disponíveis para criar novas réplicas
                // Selecionar DataNodes que não estejam na lista de réplicas
                List<String> availableNodes = new ArrayList<>(dataNodes.keySet());
                availableNodes.removeAll(replicas);  // remover nós já contendo a parte
                availableNodes.remove(failedNodeId); // remover o nó falho
                // O sourceNodeId já está em replicas, então não há problema duplicar
    
                for (String candidateNodeId : availableNodes) {
                    if (replicasNeeded <= 0) {
                        break; // já alcançamos o número necessário de réplicas
                    }
    
                    DataNodeInterface candidateNode = dataNodes.get(candidateNodeId);
                    if (candidateNode == null) {
                        continue;
                    }
    
                    try {
                        if (candidateNode.uploadPart(imageName, partNumber, partData)) {
                            replicas.add(candidateNodeId);
                            replicasNeeded--;
                            System.out.println("Criada nova réplica da parte " + partNumber 
                                               + " da imagem " + imageName 
                                               + " no DataNode " + candidateNodeId);
                        }
                    } catch (RemoteException e) {
                        System.err.println("Falha ao enviar réplica da parte " + partNumber + " da imagem " 
                                           + imageName + " para o DataNode " + candidateNodeId + ": " + e.getMessage());
                    }
                }
    
                if (replicas.size() < replicationFactor) {
                    System.err.println("Não foi possível restaurar completamente o número de réplicas da parte " 
                                       + partNumber + " da imagem " + imageName 
                                       + ". Réplicas atuais: " + replicas.size() 
                                       + " de " + replicationFactor);
                }
            }
        }
        System.out.println("Redistribuição de dados do DataNode falho " + failedNodeId + " concluída.");
    }
    
    @Override
    public boolean storeImage(String imageName, byte[] imageData, int numParts) throws RemoteException {
        try {
            // Cálculo do tamanho de cada parte
            int partSize = imageData.length / numParts;

            // Mapa para armazenar as partes e suas réplicas
            Map<Integer, List<String>> partsMap = new HashMap<>();

            // Obter a lista de DataNodes disponíveis
            List<String> dataNodeIds = new ArrayList<>(dataNodes.keySet());
            if (dataNodeIds.isEmpty()) {
                System.err.println("Nenhum DataNode disponível para armazenar a imagem.");
                return false;
            }

            // Embaralha a lista de DataNodes para distribuir as partes de forma mais
            // aleatória
            Collections.shuffle(dataNodeIds);

            // Para cada parte da imagem
            for (int i = 0; i < numParts; i++) {
                int start = i * partSize;
                int end = (i == numParts - 1) ? imageData.length : start + partSize;
                byte[] partData = Arrays.copyOfRange(imageData, start, end);

                // Vamos tentar colocar essa parte em 'replicationFactor' DataNodes distintos
                List<String> replicas = new ArrayList<>();

                // Se não houver DataNodes suficientes para atingir replicationFactor,
                // armazenaremos em quantos for possível (ideal é ter replicationFactor ≤ número
                // de DataNodes)
                int replicasToCreate = Math.min(replicationFactor, dataNodeIds.size());

                // Itera sobre a lista de DataNodes disponíveis para criar réplicas
                // Caso a lista seja menor que replicationFactor, criará menos réplicas.
                // Aqui usamos o índice 'i' para espalhar as partes, mas poderíamos usar outro
                // critério.
                for (int r = 0; r < replicasToCreate; r++) {
                    // Seleciona um DataNode com base no índice (i e r). Por exemplo:
                    // Poderíamos fazer um cálculo modular: (i + r) % dataNodeIds.size()
                    // para pegar DataNodes diferentes por parte.
                    String dataNodeId = dataNodeIds.get((i + r) % dataNodeIds.size());
                    DataNodeInterface dataNode = dataNodes.get(dataNodeId);

                    if (dataNode == null) {
                        System.err.println("DataNode " + dataNodeId
                                + " não encontrado durante o upload. Notificando o MonitorService.");
                        notifyMonitorService(dataNodeId);
                        return false;
                    }

                    try {
                        if (dataNode.uploadPart(imageName, i, partData)) {
                            replicas.add(dataNodeId);
                        } else {
                            System.err.println("Falha ao armazenar a parte " + i + " da imagem '" + imageName
                                    + "' no DataNode " + dataNodeId + ".");
                            // Se falhar em uma réplica, tenta continuar com as demais réplicas?
                            // Aqui poderíamos optar por falhar totalmente ou continuar.
                            // Vamos continuar tentando com outros DataNodes.
                        }
                    } catch (RemoteException e) {
                        System.err.println("DataNode " + dataNodeId
                                + " inacessível durante o upload. Notificando o MonitorService.");
                        notifyMonitorService(dataNodeId);
                        // Continua a tentar com outros DataNodes
                    }
                }

                if (replicas.isEmpty()) {
                    // Não conseguimos armazenar nem uma réplica dessa parte
                    System.err.println("Falha ao armazenar a parte " + i + " da imagem '" + imageName
                            + "' em quaisquer DataNodes.");
                    return false;
                }

                partsMap.put(i, replicas);
            }

            // Armazena o mapeamento de todas as partes da imagem
            imageParts.put(imageName, partsMap);
            System.out.println("Imagem '" + imageName + "' armazenada com sucesso, com fator de replicação "
                    + replicationFactor + ".");

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
        Map<Integer, List<String>> parts = imageParts.remove(imageName);
        if (parts != null) {
            for (Map.Entry<Integer, List<String>> entry : parts.entrySet()) {
                int partNumber = entry.getKey();
                List<String> replicaNodes = entry.getValue();

                // Deletar a parte em cada DataNode que a possui
                for (String dataNodeId : replicaNodes) {
                    DataNodeInterface dataNode = dataNodes.get(dataNodeId);
                    if (dataNode == null) {
                        System.err.println("DataNode " + dataNodeId
                                + " não encontrado durante a exclusão. Notificando o MonitorService.");
                        notifyMonitorService(dataNodeId);
                        continue;
                    }
                    try {
                        dataNode.deletePart(imageName, partNumber);
                    } catch (RemoteException e) {
                        System.err.println("DataNode " + dataNodeId
                                + " inacessível durante a exclusão. Notificando o MonitorService.");
                        notifyMonitorService(dataNodeId);
                    }
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

    @Override
    public void handleDataNodeFailure(String dataNodeId) throws RemoteException {
        // Remover o DataNode do mapa (se ainda não foi removido)
        dataNodes.remove(dataNodeId);

        // Verificar se a imagem possuía partes unicamente nesse DataNode.
        // Se houver replicação, tentar realocar dados.
        redistributeDataFromFailedNode(dataNodeId);
        // Caso não exista replicação, pode-se apenas marcar as imagens que dependiam
        // desse nó como corrompidas.
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
     * 
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
     * 
     * @param eventType Tipo do evento (ex: IMAGE_ADDED, IMAGE_DELETED)
     * @param imageName Nome da imagem associada ao evento
     */
    private void notifySubscribers(String eventType, String imageName) {
        List<SubscriberInterface> subs = subscribersByEventType.get(eventType);
        if (subs == null)
            return;
        synchronized (subs) {
            for (SubscriberInterface subscriber : subs) {
                try {
                    subscriber.notify(eventType, imageName);
                } catch (RemoteException e) {
                    System.err
                            .println("Falha ao notificar um assinante do evento " + eventType + ": " + e.getMessage());
                }
            }
        }
    }

    public static void main(String[] args) {
        try {
            int replicationFactor = 2;
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
