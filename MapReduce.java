import java.io.*;
import java.util.LinkedList;
import java.util.StringTokenizer;


/**
 * Created by KKV on 20.03.2017.
 */

class Node implements Comparable<Node>{
    private String key;
    private String value;

    public Node(String key, String value) {
        this.key = key;
        this.value = value;
    }
    public Node(String str){
        StringTokenizer tokenizer = new StringTokenizer(str, "\t");
        key = tokenizer.nextToken();
        value = tokenizer.nextToken();
    }

    @Override
    public int compareTo(Node o) {
        return key.compareTo(o.key);
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return  key + '\t' + value;
    }
}
public class MapReduce {

    private static void sortTransFile(String inputfile_path){
        LinkedList<Node> transFile = new LinkedList<>();
        try(BufferedReader read_from_file = new BufferedReader(new FileReader(inputfile_path)))
        {
            String line;
            while((line = read_from_file.readLine()) != null) {
                transFile.add(new Node(line));
            }

            transFile.sort(Node::compareTo);
            read_from_file.close();

            BufferedWriter write_to_file = new BufferedWriter(new FileWriter(inputfile_path));
            for (Node i : transFile){
                write_to_file.write(i.toString());
                write_to_file.newLine();
            }
            write_to_file.close();
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }
    public static void main(String[] args) {
        String comand, script_path, inputfile_path, outputfile_path;
        if (args.length < 4) {
            return;
        }

        comand = args[0];
        script_path = args[1];
        inputfile_path = args[2];
        outputfile_path = args[3];

        LinkedList<String> script_comands = new LinkedList<>();
        script_comands.add(script_path);
        for (int i = 4; i < args.length; i++) {
            script_comands.add(args[i]);
        }

        if (comand.equals("reduce")) {
            try {
                sortTransFile(inputfile_path);
                ProcessBuilder processbilder = new ProcessBuilder(script_comands);

                BufferedReader read_from_file = new BufferedReader(new FileReader(inputfile_path));
                BufferedWriter write_to_file = new BufferedWriter(new FileWriter(outputfile_path));

                String line = read_from_file.readLine();
                Node node = new Node(line);
                Node cur = new Node(line);
                while(line  != null) {
                    node = cur;
                    Process process = processbilder.start();
                    BufferedWriter write_to_process = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
                    BufferedReader read_from_process = new BufferedReader(new InputStreamReader(process.getInputStream()));

                    while (node.getKey().equals(cur.getKey())) {
                        write_to_process.write(cur.toString());
                        write_to_process.newLine();
                        line = read_from_file.readLine();
                        if (line == null) break;
                        else cur = new Node(line);
                    }

                    write_to_process.close();
                    process.waitFor();

                    String str;
                    while ((str = read_from_process.readLine()) != null) {
                        write_to_file.write(str);
                        write_to_file.newLine();
                    }
                    read_from_process.close();
                }
                read_from_file.close();
                write_to_file.close();
            } catch (IOException | InterruptedException e) {
                e.printStackTrace();
            }
        } else if (comand.equals("map")) {
            try {
                BufferedReader read_from_file = new BufferedReader(new FileReader(inputfile_path));
                BufferedWriter write_to_file = new BufferedWriter(new FileWriter(outputfile_path));
                ProcessBuilder processBilder = new ProcessBuilder(script_comands);

                String line;
                while ((line = read_from_file.readLine()) != null) {

                    Process process = processBilder.start();
                    BufferedWriter write_to_process = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()));
                    BufferedReader read_from_process = new BufferedReader(new InputStreamReader(process.getInputStream()));

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
        } else {
            try {
                throw (new Exception("Wrong parameters"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

