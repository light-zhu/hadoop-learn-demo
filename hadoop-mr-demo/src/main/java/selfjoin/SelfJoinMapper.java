package selfjoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <p>Title:SelfJoinMapper</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/3 15:17
 */
public class SelfJoinMapper extends Mapper<LongWritable, Text, IntWritable, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String data = value.toString();
        String[] words = data.split(",");

        //作为老板表
        context.write(new IntWritable(Integer.parseInt(words[0])), new Text("*" + words[1]));

        //作为员工表 输出老板号可能会产生Exception
        try {
            context.write(new IntWritable(Integer.parseInt(words[3])), new Text(words[1]));
        }catch (Exception ex){
            context.write(new IntWritable(-1), new Text(words[1]));
        }


    }
}
