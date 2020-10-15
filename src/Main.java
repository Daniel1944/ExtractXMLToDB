import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.sql.*;

public class Main {

    private static File XML_FILE;

    public static void downloadArchive() throws IOException {
        URL website = new URL("https://vdp.cuzk.cz/vymenny_format/soucasna/20200930_OB_573060_UZSZ.xml.zip");
        ReadableByteChannel rbc = Channels.newChannel(website.openStream());
        FileOutputStream fos = new FileOutputStream("information.zip");
        fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
    }

    private static void unzip(String zipFilePath, String destDir) {
        File dir = new File(destDir);
        if (!dir.exists()) dir.mkdirs();
        FileInputStream fis;
        byte[] buffer = new byte[1024];
        try {
            fis = new FileInputStream(zipFilePath);
            ZipInputStream zis = new ZipInputStream(fis);
            ZipEntry ze = zis.getNextEntry();
            while (ze != null) {
                String fileName = ze.getName();
                File newFile = new File(destDir + File.separator + fileName);
                XML_FILE = newFile;
                System.out.println("Unzipping to " + newFile.getAbsolutePath());
                new File(newFile.getParent()).mkdirs();
                FileOutputStream fos = new FileOutputStream(newFile);
                int len;
                while ((len = zis.read(buffer)) > 0) {
                    fos.write(buffer, 0, len);
                }
                fos.close();
                zis.closeEntry();
                ze = zis.getNextEntry();
            }
            zis.closeEntry();
            zis.close();
            fis.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Connection connectToDB() {

        Connection con = null;
        try {
            Class.forName("org.sqlite.JDBC");
            con = DriverManager.getConnection("jdbc:sqlite:TrixiExamDB.db");
        } catch (Exception e) {
            e.printStackTrace();
        }

        return con;
    }

    public void insertToFirstTable(int code, String name) {
        String sql = "INSERT INTO obec(kod, nazev) VALUES(?,?)";

        try (Connection con = this.connectToDB()) {
            PreparedStatement psmt = con.prepareStatement(sql);
            psmt.setInt(1, code);
            psmt.setString(2, name);
            psmt.executeUpdate();
            psmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public void insertToSecondTable(int code, String name, int codePrime) {
        String sql = "INSERT INTO cast_obce(kod, nazev, kod_obce) VALUES(?,?,?)";

        try (Connection con = this.connectToDB()) {
            PreparedStatement psmt = con.prepareStatement(sql);
            psmt.setInt(1, code);
            psmt.setString(2, name);
            psmt.setInt(3, codePrime);
            psmt.executeUpdate();
            psmt.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {

        downloadArchive();

        Path root = FileSystems.getDefault().getPath("").toAbsolutePath();
        Path zipPath = Paths.get(root.toString(), "information.zip");

        unzip(String.valueOf(zipPath), String.valueOf(root));

        Main m = new Main();

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            Document document = builder.parse(XML_FILE);

            NodeList nList = document.getElementsByTagName("vf:Obec");
            NodeList nList2 = document.getElementsByTagName("vf:CastObce");
            int code = 0;

            //prvni tabulka
            for (int i = 0; i < nList.getLength(); i++) {
                Node nNode = nList.item(i);
                System.out.println("NOde name " + nNode.getNodeName() + " " + (i + 1));
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    code = Integer.parseInt(eElement.getElementsByTagName("obi:Kod").item(0).getTextContent());
                    m.insertToFirstTable(Integer.parseInt(eElement.getElementsByTagName("obi:Kod").item(0).getTextContent()),
                            eElement.getElementsByTagName("obi:Nazev").item(0).getTextContent());
                }
            }

            //druha tabulka
            for (int i = 0; i < nList2.getLength(); i++) {
                Node nNode = nList2.item(i);
                if (nNode.getNodeType() == Node.ELEMENT_NODE) {
                    Element eElement = (Element) nNode;
                    m.insertToSecondTable(Integer.parseInt(eElement.getElementsByTagName("coi:Kod").item(0).getTextContent()),
                            eElement.getElementsByTagName("coi:Nazev").item(0).getTextContent(), code);
                }
            }

        } catch (ParserConfigurationException | SAXException e) {
            e.printStackTrace();
        }

    }
}
