package eu.ostrzyciel.jelly.benchmark

import eu.ostrzyciel.jelly.convert.jena.riot.JellyLanguage
import eu.ostrzyciel.jelly.stream.{DecoderFlow, JellyIo}
import org.apache.jena.rdf.model.{Model, ModelFactory}
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.sparql.graph.GraphFactory
import org.apache.jena.vocabulary.{DCAT, DCTerms}
import org.apache.pekko.actor.typed.ActorSystem
import org.apache.pekko.actor.typed.scaladsl.Behaviors
import org.apache.pekko.http.scaladsl.Http
import org.apache.pekko.http.scaladsl.model.*
import org.apache.pekko.http.scaladsl.model.headers.{Accept, Location}
import org.apache.pekko.stream.scaladsl.{FileIO, Sink, StreamConverters}

import java.nio.file.Path
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters.*

object DownloadDatasetsUtility:
  import eu.ostrzyciel.jelly.convert.jena.given

  given as: ActorSystem[_] = ActorSystem[Nothing](Behaviors.empty, "DownloadDatasetsUtility")
  given ExecutionContext = as.executionContext

  val rbBase = "https://w3id.org/riverbench"
  val mediaJelly = MediaRange(MediaType.applicationBinary("x-jelly-rdf", MediaType.Compressible))

  /**
   * Downloads datasets from RiverBench, in gzipped Jelly format.
   * 
   * @param profile the RiverBench profile to download datasets for (e.g., stream-mixed)
   * @param version the RiverBench version to download datasets from (e.g., 2.0.1)
   * @param size the size of the datasets to download (e.g., 10K, 100K, 1M, full)
   * @param outputDir the directory to save the downloaded datasets to
   */
  @main
  def runDownloadDatasetsUtility(profile: String, version: String, size: String, outputDir: String): Future[Unit] =
    getMetadata(f"$rbBase/v/$version/profiles/$profile") map { mainM =>
      val datasetFutures = mainM.listObjectsOfProperty(mainM.createProperty(DCAT.NS, "seriesMember"))
        .asScala
        .map(_.asResource().getURI)
        .map(iri => getMetadata(iri).map(m => (iri, m)))
        .toSeq
      Future.sequence(datasetFutures) map { datasets =>
        for (datasetIri, datasetM) <- datasets yield
          val distribution = datasetM
            .listSubjectsWithProperty(DCTerms.identifier, datasetM.createLiteral(f"jelly-$size"))
            .asScala.nextOption()
            .getOrElse({
              println(s"Dataset $datasetIri does not have jelly-$size, using jelly-full instead")
              datasetM.listSubjectsWithProperty(DCTerms.identifier, datasetM.createLiteral(f"jelly-full")).next
            })
          (
            datasetIri.split('/').dropRight(1).last,
            datasetM.listObjectsOfProperty(distribution, DCAT.downloadURL).next.asResource.getURI
          )
      } map { downloadLinks =>
        val outputPath = Path.of(outputDir)
        outputPath.toFile.mkdirs()
        for (name, url) <- downloadLinks yield
          val path = outputPath.resolve(f"$name.jelly.gz")
          getWithFollowRedirects(url, None) flatMap { response =>
            println(f"Got reponse ${response.status} for $name -- saving to $path")
            response.entity.dataBytes
              .runWith(FileIO.toPath(path))
              .map(_ => println(s"Downloaded $name"))
          }
      } map { saveFutures =>
        Future.sequence(saveFutures) map { _ =>
          println("All datasets downloaded")
          as.terminate()
        }
      }
    }

  def getMetadata(url: String): Future[Model] =
    println(s"Downloading metadata from $url")
    getWithFollowRedirects(url, Some(mediaJelly)) map { r =>
      val model = ModelFactory.createDefaultModel()
      val is = r.entity.dataBytes
        .runWith(StreamConverters.asInputStream())
      RDFDataMgr.read(model, is, JellyLanguage.JELLY)
      println(s"Fetched ${model.size()} triples")
      model
    }

  def getWithFollowRedirects(url: String, accept: Option[MediaRange] = None, n: Int = 0): Future[HttpResponse] =
    if n > 10 then
      Future.failed(new RuntimeException(s"Too many redirects for $url"))
    else
      Http().singleRequest(HttpRequest(
          uri = url,
          headers = accept match {
            case None => Nil
            case Some(v) => List(Accept(v))
          }
        ))
        .flatMap {
          // Follow redirects
          case HttpResponse(StatusCodes.Redirection(_), headers, _, _) =>
            val newUri = headers.collect { case Location(loc) => loc }.head
            getWithFollowRedirects(newUri.toString, accept, n + 1)
          case r => Future { r }
        }
