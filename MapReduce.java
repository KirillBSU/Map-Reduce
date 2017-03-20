import java.io.*;
import java.util.LinkedList;


/**
 * Created by KKV on 20.03.2017.
 */
public class MapReduce {
    private static void sortTransFile(String inputfile_path){
        LinkedList<String> transFile = new LinkedList<>();
        try(BufferedReader br = new BufferedReader(new FileReader(inputfile_path)))
        {
            String line;
            try {
                while((line = br.readLine()) != null) {
                    transFile.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }

            transFile.sort(String::compareTo);
            br.close();
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
        try(BufferedWriter bw = new BufferedWriter(new FileWriter(inputfile_path)))
        {
            for (String line:transFile) {
                bw.write(line);
                bw.newLine();
            }
            bw.close();
        }
        catch(IOException ex){
            System.out.println(ex.getMessage());
        }
    }
    public static void main(String[] args){
        String comand, script_path, inputfile_path, outputfile_path;
        if (args.length < 4)
        { return;}

        comand = args[0];
        script_path = args[1];
        inputfile_path = args[2];
        outputfile_path = args[3];

        LinkedList<String> script_comands = new LinkedList<>();
        script_comands.add(script_path);
        for (int i = 4; i < args.length; i++){
            script_comands.add(args[i]);
        }

        if (comand.equals("reduce")) {
            sortTransFile(inputfile_path);
        } else if (!comand.equals("map")){
            try {
                throw (new Exception ("Wrong parameters"));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        File input = new File(inputfile_path);
        File output = new File(outputfile_path);

        try {
        ProcessBuilder process = new ProcessBuilder(script_comands);
        process.redirectInput(input);
        process.redirectOutput(output);

        process.start();
        } catch (IOException e ) {
            e.printStackTrace();
        }
    }
}
