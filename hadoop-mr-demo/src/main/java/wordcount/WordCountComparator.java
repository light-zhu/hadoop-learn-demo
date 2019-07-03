package wordcount;


import org.apache.hadoop.io.Text;

/**
 * <p>Title:WordCountComparator</p>
 * <p>Description: </p>
 *
 * @author zhuxl
 * @version v1.0
 * @date 2019/7/1 9:16
 */
public class WordCountComparator extends Text.Comparator {
    @Override
    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
        return  -super.compare(b1, s1, l1, b2, s2, l2);
    }
}
