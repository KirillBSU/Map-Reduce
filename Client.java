/**
 * Created by KKV on 14.05.2017.
 */
import java.io.*;
import java.net.*;

public class Client {
    public static void main(String[] args) throws IOException {

        System.out.println("Welcome to Client side");

        Socket fromserver = null;
        int port;
        String servername;
        if (args.length==0) {
            servername = "localhost";
            port = 4444;
        }else{
            servername = args[0];
            port = new Integer(args[1]);
        }

        System.out.println("Connecting to... "+servername);

        fromserver = new Socket(servername,port);
        BufferedReader in  = new BufferedReader(new InputStreamReader(fromserver.getInputStream()));
        PrintWriter    out = new PrintWriter(fromserver.getOutputStream(),true);
        BufferedReader inu = new BufferedReader(new InputStreamReader(System.in));

        String fuser,fserver;

        while ((fuser = inu.readLine())!=null) {
            out.println(fuser);
            fserver = in.readLine();
            System.out.println(fserver);
            if (fuser.equalsIgnoreCase("close")) break;
            if (fuser.equalsIgnoreCase("exit")) break;
        }

        out.close();
        in.close();
        inu.close();
        fromserver.close();
    }
}
