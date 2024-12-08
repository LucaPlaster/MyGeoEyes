/*
    Sistemas Distribuídos
    
    LUCA MASCARENHAS PLASTER - 202014610
    MARCOS REGES MOTA - 202003598
*/

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream; 
import java.io.IOException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.*;

public class Client {
    private static final String DOWNLOAD_DIR = "client_downloads/";

    public static void main(String[] args) {
        try {
            // Cria o diretório de downloads se não existir
            File dir = new File(DOWNLOAD_DIR);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            Registry registry = LocateRegistry.getRegistry();
            MasterServerInterface master = (MasterServerInterface) registry.lookup("MasterServer");
            Scanner scanner = new Scanner(System.in);
            String option;

            do {
                System.out.println("\nEscolha uma opção:");
                System.out.println("1. Upload de imagem");
                System.out.println("2. Listar imagens");
                System.out.println("3. Baixar imagem");
                System.out.println("4. Deletar imagem");
                System.out.println("5. Teste de desempenho");
                System.out.println("6. Sair");
                System.out.print("Opção: ");
                option = scanner.nextLine();

                switch (option) {
                    case "1":
                        uploadImage(master, scanner);
                        break;
                    case "2":
                        listImages(master);
                        break;
                    case "3":
                        downloadImage(master, scanner);
                        break;
                    case "4":
                        deleteImage(master, scanner);
                        break;
                    case "5":
                        testPerformance(master, scanner);
                        break;
                    case "6":
                        System.out.println("Encerrando o cliente.");
                        break;
                    default:
                        System.out.println("Opção inválida.");
                }
            } while (!option.equals("6"));

        } catch (Exception e) {
            System.err.println("Erro no cliente: " + e.getMessage());
        }
    }

    private static void uploadImage(MasterServerInterface master, Scanner scanner) {
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

    private static void listImages(MasterServerInterface master) {
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

    private static void downloadImage(MasterServerInterface master, Scanner scanner) {
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

    private static void deleteImage(MasterServerInterface master, Scanner scanner) {
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

    private static void testPerformance(MasterServerInterface master, Scanner scanner) {
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
                byte[] imageData = new byte[1024 * 50]; // Imagem de 50KB
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
}
