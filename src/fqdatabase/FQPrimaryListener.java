/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fqdatabase;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import org.w3c.dom.Attr;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author HDUSER
 */
public class FQPrimaryListener implements Runnable {

    public FQPrimaryListener() {

    }

    private static final String FILE_PATH = "/home/HDUSER/questions/";

    @Override
    public void run() {
        initReceiveSockets();
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket(50001);
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
            List<String> secondaries = new ArrayList<>();
            String fileName = null;
            for (String token : tokens) {
                System.out.println("Token --- " + token);
            }
            if (tokens[0].equals("write")) {
                String questionText = tokens[1];
                fileName = tokens[tokens.length - 1].trim();
                //Everything in between is an IP Address of a secondary node
                for (int i = 2; i < tokens.length - 1; i++) {
                    secondaries.add(tokens[i].trim());
                }
                createQuestionFile(fileName, questionText);
            } else {
                System.out.println("Jose has started eating chicken!!!!!");
            }
            writeToSecondaries(secondaries, fileName, receivedData);
            if (receiveFromSecondaries(secondaries.size())) {
                //send done to cloud controller
                System.out.println("Received successfully from secondaries");
                sendDoneToController(originIP);
            } else {
                //rollback....
                rollBackCreate(fileName);
                System.out.println("Rolled back");
            }
        }
    }

    public void sendDoneToController(String originIP) {
        DatagramSocket reportSocket = null;
        try {
            reportSocket = new DatagramSocket();
        } catch (SocketException ex) {
            Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Could not create done socket");
        }
        String done = "done;" + originIP;
        byte buff[] = done.getBytes();
        DatagramPacket sendPacket;
        try {
            sendPacket = new DatagramPacket(buff, buff.length, InetAddress.getByName("172.18.16.55"), 50000);
            reportSocket.setSoTimeout(10000);
            reportSocket.send(sendPacket);
        } catch (UnknownHostException ex) {
            Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Could not create send packet");
        } catch (SocketException ex) {
            Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Could not create send packet");
        } catch (IOException ex) {
            Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
            System.out.println("Could not create send packet");
        }

    }

    public void rollBackCreate(String fileName) {
        File file = new File(fileName.trim());
        file.delete();
    }
    private DatagramSocket receiveSocket = null;

    public void initReceiveSockets() {

        try {
            receiveSocket = new DatagramSocket(50003);
            receiveSocket.setSoTimeout(10000);
        } catch (SocketException ex) {
            System.out.println("Could not create socket for receiving from secondaries!!!");

        }
    }

    public boolean receiveFromSecondaries(int n) {

        while (n > 0) {
            byte receiveBuff[] = new byte[1024];
            DatagramPacket receivePacket = new DatagramPacket(receiveBuff, receiveBuff.length);
            try {
                receiveSocket.receive(receivePacket);
            } catch (IOException ex) {
                return false;
            }
            n--;
        }
        return true;
    }

    public void writeToSecondaries(List<String> secondaryIPs, String fileName, String questionText) {
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();
        } catch (SocketException ex) {
            System.out.println("Could not create socket!!!");

        }
        byte sendBuff[] = new byte[1024];
        StringBuffer buffer = new StringBuffer();
        buffer.append("write;");
        buffer.append(fileName);
        buffer.append(";");
        buffer.append(questionText);
        for (String ip : secondaryIPs) {
            System.out.println("Secondary IP ---- " + ip);
            String sendData = buffer.toString();
            sendBuff = sendData.getBytes();
            DatagramPacket sendPacket = null;
            try {
                sendPacket = new DatagramPacket(sendBuff, sendBuff.length, InetAddress.getByName(ip), 50002);
                try {
                    socket.setSoTimeout(10000);
                } catch (SocketException ex) {
                    Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
                }
                try {
                    System.out.println("send packet string " + sendData.toString());
                    socket.send(sendPacket);
                } catch (IOException ex) {

                    Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
                }
            } catch (IOException ex) {

                Logger.getLogger(FQPrimaryListener.class.getName()).log(Level.SEVERE, null, ex);
            }

        }

        socket.close();
    }

    public void createQuestionFile(String fileName, String questionText) {
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

            // Output to console for testing
            //StreamResult consoleResult = new StreamResult(System.out);
            //transformer.transform(source, consoleResult);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
