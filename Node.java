import java.io.*;
import java.net.Socket;
import java.util.LinkedList;

/**
 * Created by KKV on 14.05.2017.
 */
class GlobalVars
{
    public static final int capacity = 100000;
}

public class Node {
    public static void main(String[] args) throws IOException {

        System.out.println("Welcome to Node");

        Socket fromserver = null;
        String script_path;

        int port;
        String servername;
        if (args.length==0) {
            servername = "localhost";
            port = 4444;
            script_path = "C:\\Users\\KKV\\Documents\\Visual Studio 2015\\Projects\\UP4sem\\Debug\\map_script.exe";
        }else{
            servername = args[0];
            port = new Integer(args[1]);
            script_path = args[2];
        }

        System.out.println("Connecting to... "+servername);

        fromserver = new Socket(servername,port);
        BufferedReader in  = new BufferedReader(new InputStreamReader(fromserver.getInputStream()));
        PrintWriter out = new PrintWriter(fromserver.getOutputStream(),true);
        BufferedReader inu = new BufferedReader(new InputStreamReader(System.in));

        String fuser,fserver;

        out.println("Node");
        while ((fserver = in.readLine())!=null) {
            map(fserver, fserver + "1", script_path);
            out.println(fserver + "1");
            //
        }

        out.close();
        in.close();
        inu.close();
        fromserver.close();
    }

    private static void map(String inputfile_path, String outputfile_path, String script_path){
        try {
            LinkedList<String> script_comands = new LinkedList<>();
            script_comands.add(script_path);
            BufferedReader read_from_file = new BufferedReader(new FileReader(inputfile_path), GlobalVars.capacity);
            BufferedWriter write_to_file = new BufferedWriter(new FileWriter(outputfile_path), GlobalVars.capacity);
            ProcessBuilder processBilder = new ProcessBuilder(script_comands);

            String line;
            while ((line = read_from_file.readLine()) != null) {

                Process process = processBilder.start();
                BufferedWriter write_to_process = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()), GlobalVars.capacity);
                BufferedReader read_from_process = new BufferedReader(new InputStreamReader(process.getInputStream()), GlobalVars.capacity);

                write_to_process.write(line);
                write_to_process.close();

                process.waitFor();

                while ((line = read_from_process.readLine()) != null) {
                    write_to_file.write(line);
                    write_to_file.newLine();
                }
                read_from_process.close();
            }
            read_from_file.close();
            write_to_file.close();

        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
