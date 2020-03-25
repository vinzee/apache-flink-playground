package com.p1;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class BroadcastDataServer {
    public static void main(String[] args) throws IOException {
        ServerSocket listener = new ServerSocket(9090);
        BufferedReader br = null;
        try {
            Socket socket = listener.accept();
            System.out.println("Got new connection: " + socket.toString());

            br = new BufferedReader(new FileReader("/home/jivesh/broadcast_small"));

            try {
                PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
                String line;

                while ((line = br.readLine()) != null) {
                    out.println(line);
                    //Thread.sleep(100);
                }
            } finally {
                socket.close();
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            listener.close();
            if (br != null)
                br.close();
        }
    }
}

