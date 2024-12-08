/*
    Sistemas Distribuídos
    
    LUCA MASCARENHAS PLASTER - 202014610
    MARCOS REGES MOTA - 202003598
*/

import java.io.File;
import java.io.FileInputStream;  
import java.io.FileOutputStream; 
import java.io.IOException;
import java.nio.file.Files;
import java.rmi.RemoteException;
import java.rmi.server.UnicastRemoteObject;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

public class Client extends UnicastRemoteObject implements SubscriberInterface {
    private static final String DOWNLOAD_DIR = "client_downloads/";
    private static MasterServerInterface master;
    private static Scanner scanner;

    // Construtor necessário devido ao UnicastRemoteObject
    protected Client() throws RemoteException {
        super();
    }

    @Override
    public void notify(String eventType, String imageName) throws RemoteException {
        System.out.println("\n[NOTIFICAÇÃO] Evento: " + eventType + " | Imagem: " + imageName);
        System.out.print("Pressione ENTER para continuar...");
        new Scanner(System.in).nextLine();
    }

    public static void main(String[] args) {
        try {
            // Cria o diretório de downloads se não existir
            File dir = new File(DOWNLOAD_DIR);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            Registry registry = LocateRegistry.getRegistry();
            master = (MasterServerInterface) registry.lookup("MasterServer");

            // Cria uma instância do cliente remoto para receber notificações
            Client client = new Client();
            // O próprio "client" já é um objeto remoto, pois estendemos UnicastRemoteObject.

            scanner = new Scanner(System.in);

            String option;
            do {
                System.out.println("\nEscolha uma opção:");
                System.out.println("1. Upload de imagem");
                System.out.println("2. Listar imagens");
                System.out.println("3. Baixar imagem");
                System.out.println("4. Deletar imagem");
                System.out.println("5. Teste de desempenho");
                System.out.println("6. Inscrever-se em um tipo de evento (subscribe)");
                System.out.println("7. Cancelar inscrição em um tipo de evento (unsubscribe)");
                System.out.println("8. Listar tipos de eventos disponíveis");
                System.out.println("9. Sair");
                System.out.print("Opção: ");
                option = scanner.nextLine();

                switch (option) {
                    case "1":
                        uploadImage();
                        break;
                    case "2":
                        listImages();
                        break;
                    case "3":
                        downloadImage();
                        break;
                    case "4":
                        deleteImage();
                        break;
                    case "5":
                        testPerformance();
                        break;
                    case "6":
                        subscribeToEvent();
                        break;
                    case "7":
                        unsubscribeFromEvent();
                        break;
                    case "8":
                        listEventTypes();
                        break;
                    case "9":
                        System.out.println("Encerrando o cliente.");
                        break;
                    default:
                        System.out.println("Opção inválida.");
                }
            } while (!option.equals("9"));

        } catch (Exception e) {
            System.err.println("Erro no cliente: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static void uploadImage() {
        try {
            System.out.print("Digite o caminho da imagem a ser enviada: ");
            String filePath = scanner.nextLine();
            File file = new File(filePath);

            if (!file.exists()) {
                System.out.println("Arquivo não encontrado.");
                return;
            }

            FileInputStream fis = new FileInputStream(file);
            byte[] imageData = new byte[(int) file.length()];
            fis.read(imageData);
            fis.close();

            System.out.print("Digite o número de partes para dividir a imagem: ");
            int numParts = Integer.parseInt(scanner.nextLine());

            if (master.storeImage(file.getName(), imageData, numParts)) {
                System.out.println("Imagem enviada com sucesso.");
            } else {
                System.out.println("Falha ao enviar a imagem.");
            }
        } catch (Exception e) {
            System.err.println("Erro ao enviar a imagem: " + e.getMessage());
        }
    }

    private static void listImages() {
        try {
            List<String> images = master.listImages();
            System.out.println("Imagens disponíveis:");
            for (String image : images) {
                System.out.println("- " + image);
            }
        } catch (Exception e) {
            System.err.println("Erro ao listar imagens: " + e.getMessage());
        }
    }

    private static void downloadImage() {
        try {
            System.out.print("Digite o nome da imagem a ser baixada: ");
            String imageName = scanner.nextLine();

            Map<Integer, DataNodeInterface> partsMap = master.getImageParts(imageName);

            if (partsMap == null) {
                System.out.println("Imagem não encontrada.");
                return;
            }

            List<byte[]> imageParts = new ArrayList<>();
            for (int i = 0; i < partsMap.size(); i++) {
                DataNodeInterface dataNode = partsMap.get(i);
                byte[] partData = dataNode.downloadPart(imageName, i);
                if (partData != null) {
                    imageParts.add(partData);
                } else {
                    System.out.println("Falha ao baixar a parte " + i + " da imagem.");
                    return;
                }
            }

            // Reconstroi a imagem
            int totalSize = imageParts.stream().mapToInt(part -> part.length).sum();
            byte[] imageData = new byte[totalSize];
            int currentIndex = 0;
            for (byte[] part : imageParts) {
                System.arraycopy(part, 0, imageData, currentIndex, part.length);
                currentIndex += part.length;
            }

            // Salva a imagem
            FileOutputStream fos = new FileOutputStream(DOWNLOAD_DIR + imageName);
            fos.write(imageData);
            fos.close();

            System.out.println("Imagem '" + imageName + "' baixada com sucesso.");
        } catch (Exception e) {
            System.err.println("Erro ao baixar a imagem: " + e.getMessage());
        }
    }

    private static void deleteImage() {
        try {
            System.out.print("Digite o nome da imagem a ser deletada: ");
            String imageName = scanner.nextLine();

            if (master.deleteImage(imageName)) {
                System.out.println("Imagem deletada com sucesso.");
            } else {
                System.out.println("Imagem não encontrada ou falha ao deletar.");
            }
        } catch (Exception e) {
            System.err.println("Erro ao deletar a imagem: " + e.getMessage());
        }
    }

    private static void testPerformance() {
        try {
            System.out.print("Digite o número de imagens para o teste (10, 50, 200): ");
            int numImages = Integer.parseInt(scanner.nextLine());

            List<String> imageNames = new ArrayList<>();
            for (int i = 1; i <= numImages; i++) {
                imageNames.add("imagem_teste_" + i + ".jpg");
            }

            // Teste de inserção
            long startInsertion = System.currentTimeMillis();
            for (String imageName : imageNames) {
                byte[] imageData = new byte[1024 * 50]; // Imagem de 50KB (exemplo)
                new Random().nextBytes(imageData);
                master.storeImage(imageName, imageData, 5);
            }
            long endInsertion = System.currentTimeMillis();
            System.out.println("Tempo de inserção de " + numImages + " imagens: " + (endInsertion - startInsertion) + " ms");

            // Teste de recuperação
            long startRetrieval = System.currentTimeMillis();
            for (String imageName : imageNames) {
                Map<Integer, DataNodeInterface> partsMap = master.getImageParts(imageName);
                if (partsMap != null) {
                    for (int i = 0; i < partsMap.size(); i++) {
                        DataNodeInterface dataNode = partsMap.get(i);
                        dataNode.downloadPart(imageName, i);
                    }
                }
            }
            long endRetrieval = System.currentTimeMillis();
            System.out.println("Tempo de recuperação de " + numImages + " imagens: " + (endRetrieval - startRetrieval) + " ms");

        } catch (Exception e) {
            System.err.println("Erro no teste de desempenho: " + e.getMessage());
        }
    }

    private static void subscribeToEvent() {
        try {
            System.out.print("Digite o tipo de evento (ex: IMAGE_ADDED, IMAGE_DELETED): ");
            String eventType = scanner.nextLine();
            master.subscribe(eventType, (SubscriberInterface) UnicastRemoteObject.exportObject(new Client(), 0));
            System.out.println("Inscrito com sucesso no evento: " + eventType);
        } catch (Exception e) {
            System.err.println("Erro ao assinar o evento: " + e.getMessage());
        }
    }

    private static void unsubscribeFromEvent() {
        try {
            System.out.print("Digite o tipo de evento (ex: IMAGE_ADDED, IMAGE_DELETED): ");
            String eventType = scanner.nextLine();
            master.unsubscribe(eventType, (SubscriberInterface) UnicastRemoteObject.exportObject(new Client(), 0));
            System.out.println("Cancelada a inscrição do evento: " + eventType);
        } catch (Exception e) {
            System.err.println("Erro ao cancelar a assinatura do evento: " + e.getMessage());
        }
    }

    private static void listEventTypes() {
        try {
            List<String> events = master.listEventTypes();
            System.out.println("Tipos de eventos disponíveis:");
            for (String evt : events) {
                System.out.println("- " + evt);
            }
        } catch (Exception e) {
            System.err.println("Erro ao listar tipos de eventos: " + e.getMessage());
        }
    }
}
