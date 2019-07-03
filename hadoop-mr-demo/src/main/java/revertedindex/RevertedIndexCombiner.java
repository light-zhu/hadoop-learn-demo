package revertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


import java.io.IOException;

/**
 * <p>Title:RevertedIndexCombiner</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/3 15:39
 */
public class RevertedIndexCombiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        int total = 0;
        for (Text v : values) {
            total += Integer.parseInt(v.toString());
        }
        String data = key.toString();

        int index = data.indexOf(":");
        String word = data.substring(0, index);
        String filename = data.substring(index + 1);

        context.write(new Text(word), new Text(filename + ":" + total));
    }
}
