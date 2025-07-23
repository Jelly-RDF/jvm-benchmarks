package eu.neverblink.jelly.benchmark.patch.util

import org.openjdk.jmh.infra.Blackhole

import java.io.OutputStream

final class BlackholeOutputStream(blackhole: Blackhole) extends OutputStream:
  private var closed: Boolean = false

  private def ensureOpen(): Unit =
    if closed then throw new java.io.IOException("Stream is closed")

  override def write(b: Array[Byte], off: Int, len: Int): Unit =
    ensureOpen()
    blackhole.consume(b)

  override def write(i: Int): Unit =
    ensureOpen()
    blackhole.consume(i)

  override def close(): Unit =
    closed = true
