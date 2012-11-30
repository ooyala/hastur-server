package com.ooyala.hastur.aggregation
{
  object Aggregation
  {
    //def segment(series:Map[String,Map[Int,Object]]) =

    def skip_name(control:Map[String,Object], name:String):Boolean =
    {
      val excludes = (control get "exclude") match {
        case None => ()
        case list:Array[String] => 
      }

      val excluded = excludes filter { _ == name }
      val included = (control get "include") filter { _ == name }

      excluded != None || included == None
    }
  }

}
