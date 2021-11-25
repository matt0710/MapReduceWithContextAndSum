package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MaxPartecipantsReducer extends Reducer<Text, Text, Text, Text> {

    public void reduce (Text text, Iterable<Text> iterable, Context context) throws IOException, InterruptedException {

        String[] row = new String[2];
        HashMap<String, Integer> map = new HashMap<>();

        for (Text t : iterable) {
            row = t.toString().split(";");
            map.put(row[0], Integer.parseInt(row[1]));
        }

        int max = Collections.max(map.values());

        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            if (entry.getValue() == max) context.write(text, new Text(entry.getKey()));
        }
    }
}
