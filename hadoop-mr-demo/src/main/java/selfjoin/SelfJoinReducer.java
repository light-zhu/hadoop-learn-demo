package selfjoin;

import org.apache.commons.math3.optim.nonlinear.scalar.noderiv.NelderMeadSimplex;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <p>Title:SelfJoinReducer</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/3 15:18
 */
public class SelfJoinReducer extends Reducer<IntWritable, Text, Text, Text> {

    @Override
    protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        //定义变量保存老板姓名和员工姓名
        String bossName = "";
        String empListName = "";
        for (Text v : values) {
            String name = v.toString();

            int index = name.indexOf("*");
            if (index >= 0) {
                bossName = name.substring(1);
            } else {
                empListName = name + ";" + empListName;
            }

        }
        if(bossName.length()>0 && empListName.length()>0)
            context.write(new Text(bossName), new Text(empListName));

    }
}
