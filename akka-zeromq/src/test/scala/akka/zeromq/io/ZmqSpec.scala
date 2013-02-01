/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.zeromq.io

import akka.testkit.{ TestProbe, AkkaSpec }
import akka.io.IO
import akka.zeromq.io.Zmq._
import annotation.tailrec
import akka.actor.ActorRef

class ZmqSpec extends AkkaSpec {

  def checkZmqInstallation =
    try {
      zmq.version match {
        case ZmqVersion(x, y, _) if x >= 3 || (x >= 2 && y >= 1) ⇒ Unit
        case version ⇒ invalidZmqVersion(version)
      }
    } catch {
      case e: LinkageError ⇒ zeromqNotInstalled
    }

  def invalidZmqVersion(version: ZmqVersion) {
    info("WARNING: The tests are not run because invalid ZeroMQ version: %s. Version >= 2.1.x required.".format(version))
    pending
  }

  def zeromqNotInstalled {
    info("WARNING: The tests are not run because ZeroMQ is not installed. Version >= 2.1.x required.")
    pending
  }

  // this must stay a def for checkZmqInstallation to work correctly
  def zmq = Zmq(system)

  "ZMQ-IO" should {
    "support pub-sub connections" in new TestSetup {
      val (clientHandler, clientConnection, serverHandler, serverConnection) = establishNewClientConnection()
    }
  }

  class TestSetup {
    val bindHandler = TestProbe()

    val endpoint = "tcp://127.0.0.1:%s" format { val s = new java.net.ServerSocket(0); try s.getLocalPort finally s.close() }

    checkZmqInstallation

    bindServer()

    def bindServer(): Unit = {
      val bindCommander = TestProbe()
      bindCommander.send(IO(Zmq), Bind(bindHandler.ref, endpoint))
      bindCommander.expectMsg(Bound)
    }

    def establishNewClientConnection(): (TestProbe, ActorRef, TestProbe, ActorRef) = {
      val connectCommander = TestProbe()
      connectCommander.send(IO(Zmq), Connect(endpoint))
      val Connected(`endpoint`) = connectCommander.expectMsgType[Connected]
      val clientHandler = TestProbe()
      connectCommander.sender ! Register(clientHandler.ref)

      //      val Connected(`localAddress`, `endpoint`) = bindHandler.expectMsgType[Connected]
      val serverHandler = TestProbe()
      bindHandler.sender ! Register(serverHandler.ref)

      (clientHandler, connectCommander.sender, serverHandler, bindHandler.sender)
    }

    @tailrec final def expectReceivedData(handler: TestProbe, remaining: Int): Unit =
      if (remaining > 0) {
        val recv = handler.expectMsgType[Received]
        expectReceivedData(handler, remaining - recv.data.size)
      }
  }

}
