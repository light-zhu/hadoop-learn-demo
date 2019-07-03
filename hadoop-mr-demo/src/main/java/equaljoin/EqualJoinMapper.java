package equaljoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <p>Title:EqualJoinMapper</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/3 14:44
 */
public class EqualJoinMapper extends Mapper<LongWritable, Text,IntWritable,Text> {

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

        String data = v1.toString();

        //分词
        String[] words = data.split(",");

        if(words.length == 8){
         //处理员工表，输出 部门号： 员工名字
            context.write(new IntWritable(Integer.parseInt(words[7])),new Text(words[1]));

        }else{
            context.write(new IntWritable(Integer.parseInt(words[0])),new Text("*"+words[1]));
        }

    }
}
