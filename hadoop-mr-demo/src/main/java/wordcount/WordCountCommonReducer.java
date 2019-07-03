package wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <p>Title:WordCountSortReducer</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/6/30 15:19
 */
public class WordCountCommonReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {

        //定义计数变量
        Integer total = 0;

        //遍历相加
        for(IntWritable v : v3){
            total += v.get();
        }

        context.write(k3,new IntWritable(total));

    }
}
