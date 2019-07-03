package equaljoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import partition.MyPartitioner;

/**
 * <p>Title:EqualJoinMain</p>
 * <p>Description: </p>
 * 多表连接：输出 部门名称：员工列表
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/3 14:45
 */
public class EqualJoinMain {

    public static void main(String[] args) throws Exception {
        // 1. 创建job
        Job job = Job.getInstance(new Configuration());
        job.setJobName("totalSalaryJob");
        job.setJarByClass(EqualJoinMain.class);

        // 2. 设置mapper及其输出类型
        job.setMapperClass(EqualJoinMapper.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        // 3. 设置reduce及其输出类型
        job.setReducerClass(EqualJoinReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 4. 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 5. 运行job
        job.waitForCompletion(true);
    }
}
