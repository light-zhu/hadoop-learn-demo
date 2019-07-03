package revertedindex;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * <p>Title:RevertedIndexMapper</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/3 15:38
 */
public class RevertedIndexMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //得到数据来自哪个文件
        String path = ((FileSplit) context.getInputSplit()).getPath().toString();
        int index = path.lastIndexOf("/");
        String fileName = path.substring(index + 1);

        String[] words = value.toString().split(" ");

        for (String word : words) {
            context.write(new Text(word + ":" + fileName), new Text("1"));
        }
    }
}
