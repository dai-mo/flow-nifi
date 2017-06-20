package org.dcs.flow

import org.apache.avro.Schema
import org.dcs.api.processor.{CoreProperties, RemoteProcessor}
import org.dcs.api.service.{Connection, FlowInstance, ProcessorInstance}
import org.dcs.commons.SchemaAction
import org.dcs.flow.FlowGraph.FlowGraphNode
import org.dcs.commons.serde.AvroImplicits._
import org.dcs.commons.serde.AvroSchemaStore

/**
  * Created by cmathew on 04/08/16.
  */

object FlowGraph {

  case class FlowGraphNode(processorInstance: ProcessorInstance,
                           var children: List[FlowGraphNode],
                           var parents: List[FlowGraphNode]) {
    // Need to override hashCode since the standard case class
    // hashCode implementation will follow children and parents
    // references which may eventually refer 'this' resulting
    // in a stack overflow
    override def hashCode(): Int = processorInstance.id.hashCode

    override def toString(): String = processorInstance.toString
  }

  def buildFlowGraph(flowInstance: FlowInstance): Set[FlowGraphNode] = {

    def addNodes(connection: Connection,
                 processorInstances:List[ProcessorInstance],
                 nodeMap: Map[String, FlowGraphNode]): Map[String, FlowGraphNode] = {
      val source = processorInstances.find(pi => pi.id == connection.getSource.id)
      val destination = processorInstances.find(pi => pi.id == connection.getDestination.id)

      var updatedNodeMap: Map[String, FlowGraphNode] = nodeMap

      val sourceNode: FlowGraphNode =
        if (nodeMap.get(source.get.id).isEmpty) {
          val node = FlowGraphNode(source.get, Nil, Nil)
          updatedNodeMap = updatedNodeMap + (node.processorInstance.id -> node)
          node
        } else
          nodeMap(source.get.id)

      val destinationNode: FlowGraphNode =
        if (nodeMap.get(destination.get.id).isEmpty) {
          val node = FlowGraphNode(destination.get, Nil, Nil)
          updatedNodeMap = updatedNodeMap + (node.processorInstance.id -> node)
          node
        } else
          nodeMap(destination.get.id)

      sourceNode.children = destinationNode :: sourceNode.children
      destinationNode.parents = sourceNode :: destinationNode.parents
      updatedNodeMap
    }

    def build(processorInstances: List[ProcessorInstance],
              connections: List[Connection],
              nodeMap: Map[String, FlowGraphNode]): Set[FlowGraphNode]  = connections match {
      case Nil => nodeMap.values.toSet
      case _ => build(processorInstances,
        connections.tail,
        addNodes(connections.head,processorInstances, nodeMap))
    }

    build(flowInstance.processors, flowInstance.connections, Map())
  }

  def roots(nodes: List[FlowGraphNode]): List[FlowGraphNode] = {
    nodes.filter(node => node.parents.isEmpty)
  }

  def filter(nodes: List[FlowGraphNode], f:FlowGraphNode => Boolean): List[FlowGraphNode] =
    nodes.filter(f)

  def exec[T](nodes: List[FlowGraphNode], f: FlowGraphNode => T, result: List[T]): List[T] = nodes match {
    case List() => result
    // FIXME: why does 'nodes.map(node => f(node)) :: result' not work ?
    case _ => exec(nodes.flatMap(node => node.children), f, result ++ nodes.map(node => f(node)))
  }

  def executeBreadthFirst[T](flowInstance: FlowInstance, f: FlowGraphNode => T): List[T] = {
    exec(roots(buildFlowGraph(flowInstance).toList), f, Nil)
  }

  def executeBreadthFirstFromNode[T](flowInstance: FlowInstance, f: FlowGraphNode => T, processorId: String): List[T] = {
    exec(filter(buildFlowGraph(flowInstance).toList, (fgn: FlowGraphNode) => fgn.processorInstance.id == processorId), f, Nil)

  }

}

object FlowGraphTraversal {
  def schemaUpdate(actions: List[SchemaAction])(fgn: FlowGraphNode): Option[ProcessorInstance] = {
    val coreProperties = CoreProperties(fgn.processorInstance.properties)

    val writeSchemaId = coreProperties.writeSchemaId

    if(fgn.processorInstance.processorType == RemoteProcessor.IngestionProcessorType ||
      writeSchemaId.isEmpty) {

      writeSchemaId.filter(_.nonEmpty).foreach(wsi => AvroSchemaStore.add(wsi))

      fgn.processorInstance.properties.
        find(rsiv => rsiv._1 == CoreProperties.ReadSchemaIdKey && rsiv._2.nonEmpty).
        foreach(rsiv => AvroSchemaStore.add(rsiv._2))

      val updatedWriteSchema =
          RemoteProcessor.resolveWriteSchema(CoreProperties(fgn.processorInstance.properties), None).
            map(_.update(actions).toString())

      val finalWriteSchema =
        if(updatedWriteSchema.contains(fgn.processorInstance.properties(CoreProperties.ReadSchemaKey)))
          Some("")
        else
          updatedWriteSchema

      finalWriteSchema.foreach(uws =>
        fgn.processorInstance.
          setProperties(fgn.processorInstance.properties + (CoreProperties.WriteSchemaKey -> uws)))

      fgn.children.foreach(f =>
        updatedWriteSchema.foreach(uws =>
          f.processorInstance.
            setProperties(f.processorInstance.properties + (CoreProperties.ReadSchemaKey -> uws))))

      Some(fgn.processorInstance)

    } else None
  }
}

