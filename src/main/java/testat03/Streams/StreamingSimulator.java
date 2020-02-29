package testat03.Streams;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;

public class StreamingSimulator {

    public static void main(String[] args) throws NumberFormatException {

        try {
            ServerSocket server = new ServerSocket(9999);
            System.out.println("Server is waiting for spark to connect...");

            Socket client = server.accept();
            System.out.println("Accepted spark as client!");

            PrintWriter out = new PrintWriter(new OutputStreamWriter(client.getOutputStream()), true);

            File file = new File(args[0]);
            BufferedReader in = new BufferedReader(new FileReader(file));

            String st;
            while ((st = in.readLine()) != null) {
                out.println(st);
                Thread.sleep(1);
            }

            in.close();
            out.close();
            client.close();
            server.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
