/**
 * Copyright (C) 2009-2012 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.zeromq.io

import akka.actor._
import util.control.NonFatal
import concurrent.duration.Duration
import collection.immutable
import org.zeromq.ZMQ.{ Poller, Socket }
import akka.actor.Terminated

private[io] class ZmqPoller(manager: ActorRef, zmq: ZmqExt) extends Actor with ActorLogging {
  import ZmqPoller._
  import akka.zeromq.io.Zmq._
  import zmq.Settings._

  @volatile var connections = immutable.HashMap.empty[Int, ActorRef]
  @volatile var childrenIndex = immutable.HashMap.empty[String, Int]
  @volatile var continuePolling = true
  val sequenceNumber = Iterator.from(0)
  val pollerManagementDispatcher = context.system.dispatchers.lookup(PollerDispatcher)

  // FIXME: So this poller from the scala zeromq library has to go, since we can't register/unregister interest in
  // IN/OUT/ERR without removing and re-adding the socket and doing lots of expensive stuff
  // * We should probably go directly att the zmq_poll function in here and keep our own datas-tructures for the sockets
  // * We should also wake up the poller using a private "inproc://" socket that it listens to so it doesn't have to busy-spin
  private val poller = zmq.context.poller

  def receive: Receive = {
    case cmd: Connect ⇒
      handleConnect(cmd)

    case cmd: Bind ⇒
      handleBind(cmd)

    case Terminated(child) ⇒
      execute(unregister(child))
  }

  override def postStop() = {
    execute(new Task {
      def tryRun() {
        continuePolling = false;
      }
    })
  }

  private def handleConnect(connect: Connect) {
    import akka.zeromq.SocketType
    val socket = zmq.context.socket(SocketType.Sub)
    // FIXME: error check
    socket.connect(connect.endpoint)
    val commander = sender
    val child = spawnChild(() ⇒ new ZmqSubConnection(commander, socket, zmq))
    execute(register(child, socket))
    child ! Connected(connect.endpoint)
  }

  private def handleBind(bind: Bind) {
    import akka.zeromq.SocketType
    val socket = zmq.context.socket(SocketType.Pub)
    // FIXME: error check
    socket.bind(bind.endpoint)
    val commander = sender
    val child = spawnChild(() ⇒ new ZmqPubConnection(commander, socket, zmq))
    execute(register(child, socket))
    child ! Bound
  }

  def spawnChild(creator: () ⇒ Actor): ActorRef = {
    val ref = context.actorOf(
      props = Props(creator, dispatcher = WorkerDispatcher),
      name = sequenceNumber.next().toString)
    context.watch(ref)
    ref
  }

  //////////////// Management Tasks scheduled via the pollerManagementDispatcher /////////////

  def execute(task: Task): Unit = {
    pollerManagementDispatcher.execute(task)
    // FIXME: we should wake up the poller here using a poller private "inproc://" socket
  }

  def register(child: ActorRef, socket: Socket) =
    new Task {
      def tryRun() {
        val name = child.path.name
        // FIXME: We forcefully register interest in reading only
        // and assume that writing will always succeed
        // The poller should be rewritten to allow changes in what we are interested in
        val index = poller.register(socket, Poller.POLLIN)
        childrenIndex = childrenIndex.updated(name, index)
        connections = connections.updated(index, child)
      }
    }

  def unregister(child: ActorRef) =
    new Task {
      def tryRun() {
        val name = child.path.name
        val index = childrenIndex(name)
        childrenIndex = childrenIndex - name
        connections = connections - index
        val socket = poller.getSocket(index)
        poller.unregister(socket)
        socket.close
      }
    }

  val poll = new Task {
    val timeout: Long = PollTimeout match {
      case Duration.Zero ⇒ 0
      case Duration.Inf  ⇒ -1
      case x             ⇒ x.toUnit(zmq.pollTimeUnit).toLong
    }

    def tryRun() {
      val pres = poller.poll(timeout)
      if (pres > 0) {
        println(">>> poll returned " + pres)
        0 until poller.getSize foreach { i ⇒
          if (poller.pollin(i)) connections(i) ! SocketReadable
          if (poller.pollout(i)) connections(i) ! SocketWritable
        }
      }
      if (continuePolling) pollerManagementDispatcher.execute(this) // re-schedules poll behind all currently queued tasks
    }
  }

  pollerManagementDispatcher.execute(poll) // start poll "loop"

  abstract class Task extends Runnable {
    def tryRun()
    def run() {
      try tryRun()
      catch {
        case NonFatal(e) ⇒ log.error(e, "Error during poller management task: {}", e)
      }
    }
  }
}

private[io] object ZmqPoller {
  case object SocketReadable
  case object SocketWritable
}
