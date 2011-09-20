package akka.cluster.zookeeper

import java.util.{ List ⇒ JList }
import collection.JavaConversions._
import org.I0Itec.zkclient.{ IZkStateListener, IZkChildListener }
import org.apache.zookeeper.Watcher.Event
import akka.cluster.coordination._
import org.apache.zookeeper.data.Stat
import java.util.concurrent.ConcurrentHashMap
import org.apache.zookeeper.Watcher.Event.KeeperState
import akka.cluster.ChangeListener._
import org.apache.zookeeper.KeeperException
import akka.cluster.storage.{ StorageException, VersionedData }

class ZookeeperCoordinationClient(zkClient: AkkaZkClient) extends CoordinationClient {

  val nodeListeners = new ConcurrentHashMap[CoordinationNodeListener, IZkChildListener]()
  val connectionListeners = new ConcurrentHashMap[CoordinationConnectionListener, IZkStateListener]()

  def close() = null

  def stopListenToConnection(listener: CoordinationConnectionListener) {
    handle {
      val zkListener = connectionListeners(listener)
      zkClient.unsubscribeStateChanges(zkListener)
      connectionListeners.remove(listener)
    }
  }

  def listenToConnection(listener: CoordinationConnectionListener) {
    handle {
      val zkListener = new ZookeeperCoordinationConnectionListener(listener)
      zkClient.subscribeStateChanges(zkListener)
      connectionListeners += (listener -> zkListener)
      ()
    }
  }

  def stopListenTo(path: String, listener: CoordinationNodeListener) {
    handle {
      val zkListener = nodeListeners(listener)
      zkClient.unsubscribeChildChanges(path, zkListener)
      nodeListeners.remove(listener)
      ()
    }
  }

  def listenTo(path: String, listener: CoordinationNodeListener) {
    handle {
      val zkListener = new ZookeeperCoordinationNodeListener(listener)
      zkClient.subscribeChildChanges(path, zkListener)
      nodeListeners += (listener -> zkListener)
      ()
    }
  }

  def deleteRecursive(path: String): Boolean = handleWith(deleteRecursiveFailed(path)) {
    zkClient.deleteRecursive(path)
  }

  def delete(path: String): Boolean = handleWith(deleteFailed(path)) {
    zkClient.delete(path)
  }

  def getChildren(path: String): List[String] = handle {
    List(asScalaBuffer(zkClient.getChildren(path)).toArray: _*)
  }

  def forceWriteData(path: String, value: Array[Byte]): VersionedData = handle {
    val stat = new Stat()
    zkClient.writeData(path, value)
    new VersionedData(value, stat.getVersion.toLong)
  }

  def writeData(path: String, value: Array[Byte], expectedVersion: Long): VersionedData = handleWith(writeDataFailed(path)) {
    val stat = new Stat()
    zkClient.writeData(path, value, expectedVersion.toInt)
    new VersionedData(value, stat.getVersion.toLong)
  }

  def readData(path: String): VersionedData = handleWith(readDataFailed(path)) {
    val stat = new Stat()
    val data = zkClient.readData(path, stat)
    new VersionedData(data, stat.getVersion.toLong)
  }

  def createEphemeral(path: String, value: Array[Byte]) {
    handleWith(createFailed(path)) {
      zkClient.createEphemeral(path, value)
    }
  }

  def createEphemeralSequential(path: String, value: Array[Byte]) {
    handle {
      zkClient.createEphemeralSequential(path, value)
    }
  }

  def createPersistent(path: String, value: Array[Byte]) {
    handleWith(createFailed(path)) {
      zkClient.createPersistent(path, value)
    }
  }

  def exists(path: String): Boolean = handleWith(existsFailed(path)) {
    zkClient.exists(path)
  }

  /*Exception handling partial functions that map store specific exceptions to generic exceptions*/

  private def deleteFailed(key: String): PartialFunction[Exception, StorageException] = {
    case e: Exception ⇒ CoordinationClient.deleteFailed(key, e)
  }

  private def deleteRecursiveFailed(key: String): PartialFunction[Exception, StorageException] = {
    case e: Exception ⇒ CoordinationClient.deleteRecursiveFailed(key, e)
  }

  private def writeDataFailed(key: String): PartialFunction[Exception, StorageException] = {
    case e: KeeperException.BadVersionException ⇒ CoordinationClient.writeDataFailedBadVersion(key, e)
    case e: KeeperException                     ⇒ CoordinationClient.writeDataFailed(key, e)
  }

  private def readDataFailed(key: String): PartialFunction[Exception, StorageException] = {
    case e: KeeperException.NoNodeException ⇒ CoordinationClient.readDataFailedMissingData(key, e)
    case e: KeeperException                 ⇒ CoordinationClient.readDataFailed(key, e)
  }

  private def existsFailed(key: String): PartialFunction[Exception, StorageException] = {
    case e: KeeperException ⇒ CoordinationClient.existsFailed(key, e)
  }

  private def createFailed(key: String): PartialFunction[Exception, StorageException] = {
    case e: KeeperException.NodeExistsException ⇒ CoordinationClient.createFailedDataExists(key, e)
    case e: KeeperException                     ⇒ CoordinationClient.createFailed(key, e)
  }

}

class ZookeeperCoordinationNodeListener(listener: CoordinationNodeListener) extends IZkChildListener {
  def handleChildChange(path: String, children: JList[String]) {
    listener.handleChange(path, List(children: _*))
  }
}

class ZookeeperCoordinationConnectionListener(listener: CoordinationConnectionListener) extends IZkStateListener {
  def handleNewSession() {
    listener.handleEvent(NewSession)
  }

  def handleStateChanged(state: Event.KeeperState) {
    state match {
      case KeeperState.SyncConnected ⇒
        listener.handleEvent(ThisNode.Connected)
      case KeeperState.Disconnected ⇒
        listener.handleEvent(ThisNode.Disconnected)
      case KeeperState.Expired ⇒
        listener.handleEvent(ThisNode.Expired)
    }
  }
}

