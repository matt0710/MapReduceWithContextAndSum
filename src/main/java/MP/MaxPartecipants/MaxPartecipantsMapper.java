package MP.MaxPartecipants;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MaxPartecipantsMapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{

    boolean check = true;
    String regex = "[0-9]+";
    Pattern p = Pattern.compile(regex);

    public void map(LongWritable longWritable, Text value, Context context) throws IOException, InterruptedException {
        String[] row = value.toString().split(";");

        if (check) {
            check = false;
            return;
        }
        //18 senfing code,  22

        if (row.length!=24) return;
        if (row[18].equals("-") || row[22].equals("-")) return;
        Matcher m = p.matcher(row[23]);

        if (!m.matches()) return;
        //if (row[17].equals("? Unknown ?")) return;

        StringBuilder str = new StringBuilder();
        str.append(row[22] + ";" + row[23]);

        context.write(new Text(row[18]), new Text(str.toString()));
    }

}
