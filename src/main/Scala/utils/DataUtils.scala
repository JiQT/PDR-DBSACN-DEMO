package utils

import java.text.SimpleDateFormat
import java.util.Date

/**
 * @Time：2021/6/29
 * @Author：JiQT
 * @File：DataUtils
 * @Software：IDEA
 **/
object DataUtils {
  //获取当前时间(yyyy-MM-dd HH:mm:ss)
    def NowDate(): String = {
      val now: Date = new Date()
      val dateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      val date = dateFormat.format(now)
      date
    }
}
