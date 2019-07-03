package totalsalary;

import javafx.scene.chart.PieChart;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * <p>Title:TotalSalaryMapper</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/6/30 21:18
 */
public class TotalSalaryMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    @Override
    protected void map(LongWritable k1, Text v1, Context context) throws IOException, InterruptedException {
        //7369,SMITH,CLERK,7902,1980/12/17,800,,20
        String data = v1.toString();

        String[] words = data.split(",");

        context.write(new IntWritable(Integer.parseInt(words[7])),
                new IntWritable(Integer.parseInt(words[5])));
    }
}
