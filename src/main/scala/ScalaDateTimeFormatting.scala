
object ScalaDateTimeFormatting {

  def main(args: Array[String]) {
      println("Hello, world!")
      import java.time.LocalDateTime
      println(LocalDateTime.now().toString())
      
      import java.time.format.DateTimeFormatter
      val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss")
      println(LocalDateTime.now().format(formatter))
      
      import java.text.SimpleDateFormat
      import java.util.{Calendar, TimeZone}
      //TimeZone timeZone = TimeZone.getTimeZone("UTC");
      println(s"__${new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime)}_")
   }
}
