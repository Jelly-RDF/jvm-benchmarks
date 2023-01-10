import eu.ostrzyciel.jelly.core.proto.v1.RdfStreamFrame

@main
def main(): Unit =
  val test = RdfStreamFrame(Seq())

  println(test)