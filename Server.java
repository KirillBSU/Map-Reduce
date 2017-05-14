/**
 * Created by KKV on 14.05.2017.
 */
import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

class InOut{
    private BufferedReader in;
    private PrintWriter out;

    public InOut(BufferedReader in, PrintWriter out) {
        this.in = in;
        this.out = out;
    }

    public BufferedReader getIn() {
        return in;
    }

    public PrintWriter getOut() {
        return out;
    }
}

class MyThread extends Thread{
    private Socket localfromclient;

    public MyThread(Socket localfromclient) {
        this.localfromclient = localfromclient;
    }

    public void run()//Этот метод будет выполняться в побочном потоке
    {

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(localfromclient.getInputStream()));

            PrintWriter out = new PrintWriter(localfromclient.getOutputStream(), true);
            String input, output;

            System.out.println("Wait for messages");
            while ((input = in.readLine()) != null) {
                if (input.equals("Node")){
                    runNode(in, out);
                    break;
                }
                if (input.equalsIgnoreCase("exit")) break;

                if (Server.filenames.containsKey(input)){
                    output = get_file_on_server(input);
                }else{
                    output = "There is no such a file";
                }

                out.println(output);
                System.out.println(output);

            }
            out.close();
            in.close();
            localfromclient.close();
            System.out.println("Привет из побочного потока!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void runNode(BufferedReader in, PrintWriter out){
        String nodename = localfromclient.getInetAddress().toString();
        System.out.println("Node " + nodename+ " has launched");

        if (Server.nodes.containsKey(nodename)){
            return;
        }
        Server.nodes.put(nodename, new InOut(in, out));
        int i = 0;
        while(i < 10) {
        i+=0;
        }
    }

    private String get_file_on_server(String filename) throws IOException {

        BufferedWriter tofile = new BufferedWriter(new FileWriter("res_of_map_"+filename));

        tofile.write("Resulting files on nodes after mapping " + filename);
        tofile.newLine();
        tofile.write(" Node \t filename ");
        tofile.newLine();

        ArrayList<String> adreses = Server.filenames.get(filename);
        for (String str:adreses){

            InOut temp = Server.nodes.get(str);
            BufferedReader in = temp.getIn();
            PrintWriter out = temp.getOut();
            out.println(filename);

            String input = in.readLine();

            tofile.write(str + "\t" + input);
            tofile.newLine();
        }

        tofile.close();
        return "results of mapping "+filename + " are in file " + "res_of_map_"+filename + " on master.";
    }
}

public class Server {
    private static Socket  fromclient = null;
    public static HashMap<String, ArrayList<String> > filenames = new HashMap<>(); // filename -> list of nodes(adree)

    public static HashMap<String, InOut > nodes = new HashMap<>();//(actual nodes) adres -> pair <in, out>

    public static void main(String[] args) throws IOException {
        System.out.println("Welcome to Server side");

        parse_file();

        ServerSocket servers = null;

        int port;
        if (args.length==0) {
            port = 4444;
        }else{
            port = new Integer(args[0]);
        }

        // create server socket
        try {
            servers = new ServerSocket(port);
        } catch (IOException e) {
            System.out.println("Couldn't listen to port" + port);
            System.exit(-1);
        }

        int i = 0;

        while (i < 10) {
            try {
                System.out.print("Waiting for a client...");
                fromclient = servers.accept();
                System.out.println("Client connected");
            } catch (IOException e) {
                System.out.println("Can't accept");
                System.exit(-1);
            }

            MyThread myThready = new MyThread(fromclient);
            myThready.start();

        }
        servers.close();
    }

    private static void parse_file()throws IOException{
        BufferedReader reader = new BufferedReader(new FileReader("files-nodes.txt"));

        String line;
        while((line = reader.readLine())!=null){

            StringTokenizer tokenizer = new StringTokenizer(line, " ");
            String key = tokenizer.nextToken();
            ArrayList<String> nodes = new ArrayList<>();

            while(tokenizer.hasMoreTokens()){
                nodes.add(tokenizer.nextToken());
            }

            filenames.put(key, nodes);
        }
    }
}
