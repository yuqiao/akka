/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.zeromq.io

import akka.util.ByteString
import akka.io.IO
import akka.actor._
import org.zeromq.{ ZMQ ⇒ JZMQ }
import org.zeromq.ZMQ.{ Socket, Poller }
import com.typesafe.config.Config
import concurrent.duration.{ Duration, FiniteDuration }
import java.util.concurrent.TimeUnit
import akka.zeromq.SocketType

object Zmq extends ExtensionKey[ZmqExt] {

  // Java API
  override def get(system: ActorSystem): ZmqExt = system.extension(this)

  private val minVersionString = "2.1.0"
  private val minVersion = JZMQ.makeVersion(2, 1, 0)

  /// COMMANDS
  sealed trait Command

  case class Connect(endpoint: String) extends Command
  case class Bind(handler: ActorRef,
                  endpoint: String)
  case class Register(handler: ActorRef) extends Command
  case object Unbind extends Command

  sealed trait CloseCommand extends Command
  case object Close extends CloseCommand

  case object NoAck

  /**
   * Write data to the ZMQ connection. If no ack is needed use the special
   * `NoAck` object.
   */
  case class Write(data: ByteString, ack: Any) extends Command {
    require(ack != null, "ack must be non-null. Use NoAck if you don't want acks.")

    def wantsAck: Boolean = ack != NoAck
  }
  object Write {
    val Empty: Write = Write(ByteString.empty, NoAck)
    def apply(data: ByteString): Write =
      if (data.isEmpty) Empty else Write(data, NoAck)
  }

  /// EVENTS
  sealed trait Event

  case class Received(data: ByteString) extends Event
  case class Connected(endpoint: String) extends Event
  case object Bound extends Event
  case object Unbound extends Event
  sealed trait ConnectionClosed extends Event
}

class ZmqExt(system: ExtendedActorSystem) extends IO.Extension {

  val Settings = new Settings(system.settings.config.getConfig("akka.io.zmq"))

  class Settings private[ZmqExt] (config: Config) {
    import config._

    val NrOfPollers = getInt("nr-of-pollers")
    val PollTimeout = getString("poll-timeout") match {
      case "infinite" ⇒ Duration.Inf
      case x          ⇒ Duration(x)
    }
    val PollerDispatcher = getString("poller-dispatcher")
    val WorkerDispatcher = getString("worker-dispatcher")
    val ManagementDispatcher = getString("management-dispatcher")
    val TraceLogging = getBoolean("trace-logging")

    require(PollTimeout >= Duration.Zero, "poll-timeout must not be negative")

    private[this] def getIntBytes(path: String): Int = {
      val size = getBytes(path)
      require(size < Int.MaxValue, s"$path must be < 2 GiB")
      size.toInt
    }
  }

  /**
   * The version of the ZeroMQ library
   * @return a [[akka.zeromq.io.ZmqVersion]]
   */
  def version: ZmqVersion = ZmqVersion(JZMQ.getMajorVersion, JZMQ.getMinorVersion, JZMQ.getPatchVersion)

  val pollTimeUnit = if (version.major >= 3) TimeUnit.MILLISECONDS else TimeUnit.MICROSECONDS

  val manager = {
    system.asInstanceOf[ActorSystemImpl].systemActorOf(
      props = Props(new ZmqManager(this)), name = "IO-ZMQ")
  }

  val context = ZmqContext()
}

/**
 * A Model to represent a version of the zeromq library
 * @param major
 * @param minor
 * @param patch
 */
case class ZmqVersion(major: Int, minor: Int, patch: Int) {
  override def toString: String = "%d.%d.%d".format(major, minor, patch)
}

/**
 * Companion object for a ZeroMQ I/O thread pool
 */
object ZmqContext {
  def apply(numIoThreads: Int = 1): ZmqContext = new ZmqContext(numIoThreads)
}

/**
 * Represents an I/O thread pool for ZeroMQ sockets.
 * By default the ZeroMQ module uses an I/O thread pool with 1 thread.
 * For most applications that should be sufficient
 *
 * @param numIoThreads
 */
class ZmqContext(numIoThreads: Int) extends {
  private val context = JZMQ.context(numIoThreads)

  def socket(socketType: SocketType.ZMQSocketType): Socket = context.socket(socketType.id)

  def poller: Poller = context.poller()

  def term: Unit = context.term
}

