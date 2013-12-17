import java.io.*;
import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Created with IntelliJ IDEA.
 * User: Dror
 * Date: 16/12/13
 * Time: 16:23
 * To change this template use File | Settings | File Templates.
 */
public class ReadCVS
{
    BufferedReader br;
    String cvsSplitBy;

    public ReadCVS()
    {
        this.br = null;
        this.cvsSplitBy = ",";
    }

    public void readFile(String filePath) throws FileNotFoundException
    {
        br = new BufferedReader(new FileReader(filePath));
    }

    public String[] getNextRecord()
    {
        String line = "";
        String[] record = null;
        try
        {
            if((line = br.readLine()) != null)
            {
                 record = (line.split(cvsSplitBy));
                for (int i = 0 ; i < record.length ; i++)
                {
                    record[i] = record[i].trim();
                }
            }

        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        return record;
    }

    public void closeFile()
    {
        try
        {
            this.br.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

}

