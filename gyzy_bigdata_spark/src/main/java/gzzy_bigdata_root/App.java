package gzzy_bigdata_root;

import java.util.Stack;

/**
 * Hello world!
 *
 */



public class App 
{   Stack<Integer> stack1 = new Stack<Integer>();
    Stack<Integer> stack2 = new Stack<Integer>();

    public static void main( String[] args )
    {


    }


















   












    public static String replaceSpace(StringBuffer str) {
        return str.toString().replace(" ", "%20");
    }
    public static boolean Find(int target, int [][] array) {
        int arraySize = array.length;
        int size = array[0].length;
        System.out.println(size);
        System.out.println(arraySize);
        for(int i=0;i<arraySize;i++){
            for(int j=0;j<size;j++){
                System.out.println("i:"+i);
                System.out.println("j:"+j);
                if(array[i][j]==target){
                    return true;
                }
            }
        }
        return false;
    }

}
