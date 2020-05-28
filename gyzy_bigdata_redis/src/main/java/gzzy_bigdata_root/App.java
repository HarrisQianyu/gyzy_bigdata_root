package gzzy_bigdata_root;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {

        System.out.println( minNumberInRotateArray(new int[]{3,4,5,1,2}) );
    }

    /**
     *  [1,2,3,4,5,6]
     *  [3,4,5,6,1,2]
     *  [1,2,3,4,5,6,7]
     *  [4,5,6,7,1,2,3]
     *  [1,2,2,2,3,3]
     *  [2,3,3,1,2,2]
     *  start < end  ,最小值在左边
     *  start > end, 最小值在右边
     *  mid
     *
     * @param array
     * @return
     */
    public static int minNumberInRotateArray(int [] array) {

        int start = 0;
        int end = array.length-1;
        int mid = array.length/2-1;
        //一直循环找到array[start]==array[end]
        while (array[start]!=array[end]){
            //防止10111的情况，会错过0最小值
            if (array[start]<array[end]){
                return array[start];
            }

            if (array[start]>array[end]){
                start = mid+1;
                end -=1;
            }

            start++;
            end--;
            mid=(start-end)/2+start;
        }
        return array[start];
    }

}
