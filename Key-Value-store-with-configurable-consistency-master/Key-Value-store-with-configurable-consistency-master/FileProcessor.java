import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;

import java.io.IOException;

public class FileProcessor {
    /**
     *
     * @param name
     * @return
     */
    public BufferedReader readerDesc(String name) {
        FileInputStream fp = null;
        BufferedReader reader = null;
        try {
            fp = new FileInputStream(name);
            reader = new BufferedReader(new InputStreamReader(fp));
        } catch (FileNotFoundException e) {
            System.out.println("File Not found, please make sure the input file available");
            e.printStackTrace();
            System.exit(0);
        } finally {

        }
        return reader;
    }

    /**
     *
     * @param reader
     * @return
     */
    public String readLine(BufferedReader reader) {
        if (reader == null) {
            return null;
        }
        String line = null;
        try {
            line = reader.readLine();
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Read a line error");
            System.exit(0);
        } finally {

        }
        return line;
    }

    @Override
    public String toString() {
        return "FileProcessor{}";
    }
}
