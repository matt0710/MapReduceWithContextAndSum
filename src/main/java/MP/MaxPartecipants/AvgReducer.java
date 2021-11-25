package MP.MaxPartecipants;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.LinkedList;

public class AvgReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    public void reduce (Text text, Iterable<IntWritable> iterable, Context context) throws IOException, InterruptedException {

        int size = 0;
        LinkedList<Integer> list = new LinkedList<>();
        int outcome = 0;

        for (IntWritable it : iterable) {
            list.add(it.get());
            size++;
        }

        for (int i=0; i<list.size(); i++) {
            outcome += list.get(i);
        }

        context.write(text, new IntWritable(outcome/size));
    }
}
