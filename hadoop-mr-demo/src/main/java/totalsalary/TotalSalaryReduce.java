package totalsalary;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <p>Title:TotalSalaryReduce</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/6/30 21:23
 */
public class TotalSalaryReduce extends Reducer<IntWritable,IntWritable,IntWritable,IntWritable> {

    @Override
    protected void reduce(IntWritable k3, Iterable<IntWritable> v3, Context context) throws IOException, InterruptedException {

        Integer total = 0;

        for(IntWritable v : v3){
            total += v.get();
        }

        context.write(k3, new IntWritable(total));
    }
}
