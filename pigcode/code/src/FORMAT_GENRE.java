/**
 * Created by xiezebin on 4/14/16.
 */

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.pig.EvalFunc;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class FORMAT_GENRE extends EvalFunc <String>
{
    @Override
    public String exec(Tuple input)
    {
        try
        {
            if (input == null || input.size() == 0)
            {
                return null;
            }

            String line = (String) input.get(0);

            String[] genres = line.split("\\|");
            int len = genres.length;
            StringBuilder sb = new StringBuilder();
            sb.append(" <zxx140430>");

            for (int i = len; i > 0; i--)
            {
                String comma = ", ";
                if (i == len)
                {
                    comma = " & ";
                }
                sb.insert(0, comma + i + ") " + genres[i - 1]);
            }
            sb.delete(0, 2);

            return sb.toString();
        }
        catch (ExecException ex) {
            System.out.println("Error: " + ex.toString());
        }

        return null;
    }
}
