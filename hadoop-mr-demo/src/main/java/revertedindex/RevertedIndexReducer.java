package revertedindex;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * <p>Title:RevertedIndexReducer</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/3 15:39
 */
public class RevertedIndexReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String str = "";
        for(Text v:values){
            str = str + "("+v.toString()+")";
        }

        context.write(key,new Text(str));
    }
}
