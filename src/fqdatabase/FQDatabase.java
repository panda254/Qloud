/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package fqdatabase;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.StringTokenizer;


/**
 *
 * @author HDUSER
 */
public class FQDatabase {

    /**
     * @param args the command line arguments
     */
    public FQDatabase() {
        Thread primaryListenerThread = new Thread(new FQPrimaryListener());
        primaryListenerThread.start();
        Thread secondaryListenerThread = new Thread(new FQSecondaryListener());
        secondaryListenerThread.start();
    }

    public boolean createQuestion(String questionText) {
        System.out.println("Question text - " + questionText);
        String sendData = "create";
        DatagramSocket socket = null;
        try {
            socket = new DatagramSocket();
        } catch (SocketException ex) {
            System.out.println("Could not create socket!!!1");
            return false;
        }
        StringBuilder builder = new StringBuilder();
        try {
            byte buff[] = sendData.getBytes();
            DatagramPacket sendPacket = new DatagramPacket(buff, buff.length, InetAddress.getByName("172.18.16.55"), 50000);
            socket.setSoTimeout(10000);
            socket.send(sendPacket);
            byte receiveBuff[] = new byte[1024];
            DatagramPacket receivePacket
                    = new DatagramPacket(receiveBuff, receiveBuff.length);
            socket.receive(receivePacket);
            String receivedStuff = new String(receivePacket.getData());
            String[] tokens = receivedStuff.trim().split(";");
            System.out.println("Receivedstuff = " + receivedStuff);
            String primaryAddress = null;   
            if (tokens[0].trim().equals("handle")) {
                primaryAddress = tokens[1];
                builder.append("write;");
                builder.append(questionText);
                builder.append(";");
                for (int i = 2; i < tokens.length-1; i++) {
                    builder.append(tokens[i]);
                    builder.append(";");
                }
                builder.append(tokens[tokens.length -1]);
            } else {
                System.out.println("CRITICAL FAILURE! WHITE WALKERS ATTACKED");
                System.exit(1);
            }
            System.out.println("after builder - " + builder.toString());
            buff = builder.toString().getBytes();
            sendPacket = new DatagramPacket(buff, buff.length, InetAddress.getByName(primaryAddress), 50001);
            socket.send(sendPacket);
            socket.receive(receivePacket);

        } catch (IOException ex) {
            System.out.println("Write failed");
            return false;
        }
        return true;
    }

    public static void main(String[] args) {
        // TODO code application logic here
        FQDatabase db = new FQDatabase();
        if (db.createQuestion("Deep thought")) {
            System.out.println("Write successful");
        }
        else {
            System.out.println("Could not write ");
        }
        db.createQuestion("mairu");
                db.createQuestion("jose thendiii");
                       db.createQuestion("Whiteroy");
    }

}
