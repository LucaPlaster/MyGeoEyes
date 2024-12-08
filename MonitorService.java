import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

/**
 * Classe que implementa o serviço de monitoramento (MonitorService).
 * Responsável por registrar o MasterServer e receber notificações de falhas em DataNodes.
 */
public class MonitorService extends UnicastRemoteObject implements MonitorServiceInterface {

    private MasterServerInterface masterServer;

    protected MonitorService() throws RemoteException {
        super();
    }

    /**
     * Registra o MasterServer para uso futuro (por exemplo, monitoramento mais elaborado).
     * @param master Referência remota para o MasterServer
     * @throws RemoteException Em caso de erro de comunicação RMI
     */
    @Override
    public void registerMasterServer(MasterServerInterface master) throws RemoteException {
        this.masterServer = master;
        System.out.println("MasterServer registrado no MonitorService.");
    }

    /**
     * Notifica o MonitorService sobre a falha de um DataNode.
     * Aqui apenas exibimos uma mensagem, mas em um cenário real,
     * poderíamos tentar isolar o DataNode problemático ou acionar algum mecanismo de recuperação.
     * @param dataNodeId O identificador do DataNode com falha
     * @throws RemoteException Em caso de erro de comunicação RMI
     */
    @Override
    public void notifyFailure(String dataNodeId) throws RemoteException {
        System.out.println("MonitorService: Falha detectada no DataNode " + dataNodeId);
        // Lógica adicional de tratamento poderia ser inserida aqui.
    }

    public static void main(String[] args) {
        try {
            MonitorService monitor = new MonitorService();
            Registry registry = LocateRegistry.createRegistry(2000);
            registry.rebind("MonitorService", monitor);
            System.out.println("MonitorService iniciado e registrado no RMI Registry na porta 2000.");
        } catch (RemoteException e) {
            System.err.println("Erro ao iniciar o MonitorService: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
