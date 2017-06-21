package main;

import parser.QueryParser;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * @author I. Marcu
 */
public class SGBDApplication {
    private final QueryParser queryParser;


    public SGBDApplication() {
        queryParser = new QueryParser();
    }


    public void run() {
        boolean running = true;
        while (running) {
            try {
                final String query = readQuery();
                if (!query.equalsIgnoreCase("exit")) {
                    String results = queryParser.parse(query);
                    System.out.println(results);
                } else {
                    running = false;
                }
            } catch (IOException e) {
                e.getMessage();
            }
        }
    }

    private  String readQuery() throws IOException {
        System.out.print("Q: ");
        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        return br.readLine();
    }
}
