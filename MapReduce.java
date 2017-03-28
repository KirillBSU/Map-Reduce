import java.io.*;
import java.util.LinkedList;
import java.util.StringTokenizer;
import java.util.Vector;

class GlobalVars
{
    public static final int capacity = 100000;
}

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

class ClearingFile{
    public static void clearingFile(String filepath){
        try {
            BufferedWriter out = new BufferedWriter(new FileWriter(filepath));
            out.write("");
            out.close();
        } catch (Exception e)
        {e.printStackTrace();}
    }
}

class SortBigFiles {
    private Vector<String> pathes = new Vector<>();
    private String final_filepath;

    public SortBigFiles(String inputfile_path) {
        try {
            RandomAccessFile input_file = new RandomAccessFile(inputfile_path, "rw");
            Vector<Node> nodeVector = new Vector<>();
            pathes.add("output0.txt");

            while(input_file.getFilePointer()!= input_file.length()){
                String content = read(input_file, GlobalVars.capacity);
                tokenize(content, nodeVector);
                nodeVector.sort(Node::compareTo);
                write_pathes(nodeVector);
                nodeVector.clear();
            }

            final_filepath = starting_merge();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    public String getFinal_filepath() {
        return final_filepath;
    }

    private String read(RandomAccessFile file, int cap) throws IOException {
        byte[] bytearray = new byte[cap];
        file.read(bytearray);
        String str =file.readLine();
        if (str!=null)
            return new String(bytearray) + str;
        return new String(bytearray);
    }


    private void tokenize(String cont, Vector<Node> vector){
        StringTokenizer tokenizer = new StringTokenizer(cont, "\t\n\r\u0000");
        while(tokenizer.hasMoreTokens()){
            vector.add(new Node(tokenizer.nextToken(), tokenizer.nextToken()));
        }
    }

    private void write_pathes(Vector<Node> vector) throws IOException {
        int curIdx = pathes.size();
        pathes.add("output"+ String.valueOf(curIdx) + ".txt" );
        File write_file = new File(pathes.elementAt(curIdx));
        BufferedWriter write_buffer = new BufferedWriter(new FileWriter(write_file), GlobalVars.capacity);
        for (Node i : vector){
            write_buffer.write(i.toString());
            write_buffer.newLine();
        }
        write_buffer.close();
    }

    private String starting_merge() throws IOException {
        while(pathes.size() > 2) {
            merge(pathes.elementAt(1), pathes.elementAt(2), pathes.elementAt(0));
            String tmp = pathes.elementAt(1);
            pathes.set(1, pathes.elementAt(0));
            pathes.set(0, tmp);

            int i = 3;
            int last_actual = 1;
            for (; i < pathes.size(); i+=2){
                if (i + 1 < pathes.size()){
                    merge(pathes.elementAt(i), pathes.elementAt(i+1), pathes.elementAt((i+1)/2));
                }else{
                    tmp = pathes.elementAt((i+1)/2);
                    pathes.set((i+1)/2, pathes.elementAt(i));
                    pathes.set(i, tmp);
                }
                last_actual = (i+1)/2;
            }
            for (int j = pathes.size() - 1; j > last_actual; j--){
                ClearingFile.clearingFile(pathes.elementAt(j));
                pathes.removeElementAt(j);
            }
        }
        ClearingFile.clearingFile(pathes.elementAt(0));
        return pathes.elementAt(1);
    }

    private void merge(String file1_path, String file2_path, String res_file_path) throws IOException {
        RandomAccessFile file1 = new RandomAccessFile(file1_path, "rw");
        Vector<Node> vector1 = new Vector<>();
        RandomAccessFile file2 = new RandomAccessFile(file2_path, "rw");
        Vector<Node> vector2 = new Vector<>();

        int i = 0; int j = 0;
        BufferedWriter write_buffer = new BufferedWriter(new FileWriter(res_file_path), GlobalVars.capacity);
        while(1 == 1){
            while(i < vector1.size() && j < vector2.size()){
                if (vector1.elementAt(i).compareTo(vector2.elementAt(j)) < 0){
                    write_buffer.write(vector1.elementAt(i).toString());
                    write_buffer.newLine();
                    i++;
                }else{
                    write_buffer.write(vector2.elementAt(j).toString());
                    write_buffer.newLine();
                    j++;
                }
            }

            if (i == vector1.size()) {
                if (!check_and_read(file1, file2, vector1, vector2, j, write_buffer))
                    break;
                else i = 0;
            }
            if (j == vector2.size()){
                if (!check_and_read(file2, file1, vector2, vector1, i, write_buffer))
                    break;
                else j = 0;
            }
        }
        write_buffer.close();
    }

    private boolean check_and_read(RandomAccessFile file_for_check, RandomAccessFile anotherfile,
                                   Vector<Node> vector_ended, Vector<Node> another_vector, int another_idx, BufferedWriter write_buffer) throws IOException {
        if (file_for_check.getFilePointer()== file_for_check.length()) {
            write_from_idx(another_vector, write_buffer, another_idx);
            write_till_eof(anotherfile, another_vector, write_buffer);
            return false;
        }else {
            vector_ended.clear();
            String cont = read(file_for_check, GlobalVars.capacity);
            tokenize(cont, vector_ended);
            return  true;
        }
    }

    private void write_till_eof(RandomAccessFile file, Vector<Node> vector, BufferedWriter write_buffer) throws IOException {
        while(file.getFilePointer()!= file.length()){
            String cont = read(file, GlobalVars.capacity);
            tokenize(cont, vector);
            for (Node k : vector){
                write_buffer.write(k.toString());
                write_buffer.newLine();
            }
            vector.clear();
        }
    }
    private void write_from_idx(Vector<Node> vector, BufferedWriter write_buffer, int idx) throws IOException {
        for (; idx < vector.size(); idx++) {
            write_buffer.write(vector.elementAt(idx).toString());
            write_buffer.newLine();
        }
        vector.clear();
    }
}

public class MapReduce {

    private static void sortTransFile(String inputfile_path){
        LinkedList<Node> transFile = new LinkedList<>();
        try(BufferedReader read_from_file = new BufferedReader(new FileReader(inputfile_path), GlobalVars.capacity))
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
                SortBigFiles bigfile = new SortBigFiles(inputfile_path);
                inputfile_path = bigfile.getFinal_filepath();
                ProcessBuilder processbilder = new ProcessBuilder(script_comands);

                BufferedReader read_from_file = new BufferedReader(new FileReader(inputfile_path), GlobalVars.capacity);
                BufferedWriter write_to_file = new BufferedWriter(new FileWriter(outputfile_path), GlobalVars.capacity);

                String line = read_from_file.readLine();
                Node node = new Node(line);
                Node cur = new Node(line);
                while(line  != null) {
                    node = cur;
                    Process process = processbilder.start();
                    BufferedWriter write_to_process = new BufferedWriter(new OutputStreamWriter(process.getOutputStream()), GlobalVars.capacity);
                    BufferedReader read_from_process = new BufferedReader(new InputStreamReader(process.getInputStream()), GlobalVars.capacity);

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
        } else {
            try {
                throw (new Exception("Wrong parameters"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}

