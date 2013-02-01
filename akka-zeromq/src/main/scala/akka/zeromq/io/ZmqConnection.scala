/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.zeromq.io

import akka.actor.{ ActorRef, ActorLogging, Actor }
import org.zeromq.ZMQ.Socket
import annotation.tailrec
import akka.util.ByteString
import org.zeromq.{ ZMQ ⇒ JZMQ }
import akka.zeromq.io.ZmqPoller.SocketReadable
import akka.zeromq.io.Zmq.{ Connected, Bound }

private[io] abstract class ZmqConnection(val socket: Socket,
                                         val zmq: ZmqExt) extends Actor with ActorLogging {
  import akka.zeromq.io.Zmq._
  import zmq.Settings._

  def waitingForRegistration: Receive = {
    case Register(handler) ⇒
      if (TraceLogging) log.debug("{} registered as connection handler", handler)
      doRead(handler) // immediately try reading

      context.watch(handler) // sign death pact

      context.become(connected(handler))
  }

  /** normal connected state */
  def connected(handler: ActorRef): Receive = {
    case SocketReadable ⇒ doRead(handler)

    case write: Write if write.data.isEmpty ⇒
      if (write.wantsAck)
        sender ! write.ack

    // FIXME: non blocking direct write without failure checks
    // Rewrite the zmq.poller to allow us to register/deregister interest in read/write
    case write: Write ⇒
      socket.send(write.data.toArray, JZMQ.NOBLOCK)
      if (write.wantsAck)
        sender ! write.ack
  }

  def doRead(handler: ActorRef): Unit = {
    @tailrec def receiveMessage(currentBS: ByteString = ByteString.empty): ByteString = {
      socket.recv(JZMQ.NOBLOCK) match {
        case null ⇒ /*EAGAIN*/
          if (currentBS.isEmpty) currentBS else receiveMessage(currentBS)
        case bytes ⇒
          val bs = currentBS ++ ByteString(bytes)
          if (socket.hasReceiveMore) receiveMessage(bs) else bs
      }
    }
    receiveMessage() match {
      case bs if bs.isEmpty ⇒ // nothing to do
      case bs               ⇒ handler ! Received(bs)
    }
  }

}

private[io] object ZmqConnection {
}

private[io] class ZmqPubConnection(val bindCommander: ActorRef, _socket: Socket,
                                   _zmq: ZmqExt) extends ZmqConnection(_socket, _zmq) {

  def receive: Receive = {
    case Bound ⇒
      bindCommander ! Bound
      context.become(waitingForRegistration)
  }
}
private[io] class ZmqSubConnection(val connectCommander: ActorRef, _socket: Socket,
                                   _zmq: ZmqExt) extends ZmqConnection(_socket, _zmq) {

  def receive: Receive = {
    case c: Connected ⇒
      connectCommander ! c
      context.become(waitingForRegistration)
  }
}
