package eu.ostrzyciel.jelly.benchmark.traits

import com.google.common.io.CountingOutputStream
import org.apache.commons.compress.compressors.zstandard.ZstdCompressorOutputStream
import org.apache.commons.io.output.NullOutputStream

import java.io.OutputStream
import java.util.zip.GZIPOutputStream

trait Size:
  protected final def getOs(method: String, mode: Option[String]): (OutputStream, CountingOutputStream) =
    val cos = new CountingOutputStream(NullOutputStream.INSTANCE)
    if mode.isDefined then
      (reWrapOs(method, cos), cos)
    else (cos, cos)

  protected final def reWrapOs(method: String, cos: OutputStream): OutputStream =
    if method == "gzip" then new GZIPOutputStream(cos)
    else if method == "zstd3" then new ZstdCompressorOutputStream(cos, 3)
    else if method == "zstd9" then new ZstdCompressorOutputStream(cos, 9)
    else cos
