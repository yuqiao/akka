/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.zeromq.io

import akka.actor.{ Props, ActorLogging, Actor }
import akka.routing.RandomRouter

private[io] class ZmqManager(zmq: ZmqExt) extends Actor with ActorLogging {
  import Zmq._

  val pollerPool = context.actorOf(
    props = Props(new ZmqPoller(self, zmq)).withRouter(RandomRouter(zmq.Settings.NrOfPollers)),
    name = "pollers")

  def receive = {
    case x @ (_: Connect | _: Bind) ⇒ pollerPool forward x
  }
}
