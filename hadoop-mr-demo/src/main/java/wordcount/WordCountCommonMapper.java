package wordcount;


import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <p>Title:WordCountSortMapper</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/6/30 15:18
 */
public class WordCountCommonMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {

        //将读取的v1转成string
        String data = v1.toString();

        //进行分词操作
        String[] words = data.split(" ");

        //输出<k2,v2>
        for(String word : words){
            context.write(new Text(word),new IntWritable(1));
        }

    }
}
