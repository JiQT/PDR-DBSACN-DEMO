package utils;

import java.io.*;

/**
 * @Time：2021/5/11
 * @Author：JiQT
 * @File：filecombine
 * @Software：IDEA
 **/
//使用git批量删除文件的指定行：find *.plt |xargs sed -i '1,6d'
//合并文件夹下的所有文件
public class filecombine {

    public static  void  main(String[] args) throws IOException {
        //定义输出目录
        String FileOut="C:\\Users\\JiQT\\Desktop\\专业材料\\论文准备材料\\dataset\\full2.txt";
        BufferedWriter bw=new BufferedWriter(new FileWriter(FileOut));

        //读取目录下的每个文件或者文件夹，并读取文件的内容写到目标文字中去
        File[] list = new File("C:\\Users\\JiQT\\Desktop\\专业材料\\论文准备材料\\dataset\\Data\\179\\Trajectory").listFiles();
        int fileCount = 0;
        int folderConut= 0;
        for(File file : list)
        {
            if(file.isFile())
            {
                fileCount++;
                BufferedReader br = new BufferedReader(new FileReader(file));
                String line;
                while((line=br.readLine())!=null) {
                    bw.write(line);
                    bw.newLine();
                }
                br.close();
            }else {
                folderConut++;
            }
        }
        bw.close();
        System.out.println("输入目录下文件个数为"+fileCount);
        System.out.println("输入目录下文件夹个数为"+folderConut);
    }
}