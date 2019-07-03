package equaljoin;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * <p>Title:EqualJoinReducer</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/3 14:45
 */
public class EqualJoinReducer extends Reducer<IntWritable, Text,Text,Text> {

    @Override
    protected void reduce(IntWritable k3, Iterable<Text> v3, Context context) throws IOException, InterruptedException {
        String deptName = "";
        String empListName = "";
        for(Text v : v3){
            String name = v.toString();
            int index = name.indexOf("*");
            if(index >= 0){
                //部门名称
                deptName = name.substring(1);
            }else {
                //员工名字
                empListName  =  name+ ";"+empListName;
            }

        }
        context.write(new Text(deptName),new Text(empListName));
    }
}
