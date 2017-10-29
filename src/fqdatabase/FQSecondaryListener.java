/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fqdatabase;

import java.io.File;
import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

/**
 *
 * @author HDUSER
 */
public class FQSecondaryListener implements Runnable {

    private static final String FILE_PATH = "/home/HDUSER/questions/";

    @Override
    public void run() {

        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket(50002);
        } catch (SocketException ex) {
            System.out.println("Could not create socket!!!");
        }

        while (true) {
            byte receiveBuff[] = new byte[1024];
            DatagramPacket receivePacket
                    = new DatagramPacket(receiveBuff, receiveBuff.length);
            try {
                socket.receive(receivePacket);
            } catch (IOException ex) {
                Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
                System.out.println("Error occurred while listening");
            }
            String originIP = receivePacket.getAddress().getHostAddress();
            String receivedData = new String(receivePacket.getData());
            System.out.println("Received Data - " + receivedData);
            String tokens[] = receivedData.split(";");
            String fileName = null;
            boolean status = false;
            if (tokens[0].equals("write")) {
                String questionText = tokens[2];
                fileName = tokens[1];

                status = createQuestionFile(fileName, questionText);
            } else {
                System.out.println("Jose has started eating chicken!!!!!");
            }
            //sendtoprimary
            if (status) {
                sendToPrimary(originIP, "success");
            }
        }
    }

    public boolean createQuestionFile(String fileName, String questionText) {
        try {
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.newDocument();

            // root elements
            Element rootElement = doc.createElement("question");
            doc.appendChild(rootElement);
            // set attribute to staff element
            Attr attr = doc.createAttribute("id");
            attr.setValue(fileName.trim());
            rootElement.setAttributeNode(attr);

            // staff elements
            Element body = doc.createElement("text");

            body.appendChild(doc.createTextNode(questionText.trim()));
            rootElement.appendChild(body);

            // write the content into xml file
            TransformerFactory transformerFactory = TransformerFactory.newInstance();
            Transformer transformer = transformerFactory.newTransformer();
            DOMSource source = new DOMSource(doc);
            StringBuffer buffer = new StringBuffer();
            buffer.append(FILE_PATH);

            System.out.println("Filename - " + fileName);
            buffer.append(fileName);

            String completeFilePath = buffer.toString();
            System.out.println("Complete file path - " + completeFilePath);
            File file = new File(fileName.trim() + ".xml");
            System.out.println("Full path - " + file.getAbsolutePath());

            file.createNewFile();
            StreamResult result = new StreamResult(file);
            transformer.transform(source, result);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void sendToPrimary(String originIP, String status) {
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();
        } catch (SocketException ex) {
            System.out.println("Could not create socket!!!");

        }
        byte sendBuff[] = new byte[1024];
        sendBuff = status.getBytes();
        DatagramPacket sendPacket;
        try {
            sendPacket = new DatagramPacket(sendBuff, sendBuff.length, InetAddress.getByName(originIP), 50003);
            try {
                socket.setSoTimeout(10000);
            } catch (SocketException ex) {
                Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
            }
            try {
                socket.send(sendPacket);
            } catch (IOException ex) {
                Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
            }
        } catch (UnknownHostException ex) {
            Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

}
