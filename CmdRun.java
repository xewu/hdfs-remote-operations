package io.apple.mss.hdfs;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class CmdRun {

    /**
     * execute command line in java
     * @param cmd Command line
     */
    public void execute(String cmd){
        try {
            Process p = Runtime.getRuntime().exec(cmd);
            StringBuilder output = new StringBuilder();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

            String line;
            while ((line = reader.readLine()) != null) {
                String rt_line = line + "\n";
                output.append(rt_line);
            }

            int exitVal = p.waitFor();
            if (exitVal == 0) {
                System.out.println(output);
//                System.exit(0);
            }

        } catch (IOException e) {
            System.out.println("wrong command line.");
        } catch (InterruptedException e) {
            System.out.println("interrupted.");
        }
    }
}
