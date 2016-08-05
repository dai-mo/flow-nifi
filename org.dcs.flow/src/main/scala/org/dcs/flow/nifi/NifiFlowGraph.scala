package org.dcs.flow.nifi

import org.dcs.api.service.{Connection, FlowInstance, ProcessorInstance}

/**
  * Created by cmathew on 04/08/16.
  */

object NifiFlowGraph {

  case class FlowGraphNode(processorInstance: ProcessorInstance,
                           var children: List[FlowGraphNode],
                           var parents: List[FlowGraphNode]) {
    // Need to override hashCode since the standard case class
    // hashCode implementation will follow children and parents
    // references which may eventually refer 'this' resulting
    // in a stack overflow
    override def hashCode(): Int = processorInstance.id.hashCode
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
          nodeMap.get(source.get.id).get

      val destinationNode: FlowGraphNode =
        if (nodeMap.get(destination.get.id).isEmpty) {
          val node = FlowGraphNode(destination.get, Nil, Nil)
          updatedNodeMap = updatedNodeMap + (node.processorInstance.id -> node)
          node
        } else
          nodeMap.get(destination.get.id).get

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

  def roots(nodes: Set[FlowGraphNode]): Set[FlowGraphNode] = {
    nodes.filter(node => node.parents.isEmpty)
  }

  def executeBreadthFirst[T](flowInstance: FlowInstance, f: FlowGraphNode => T): List[T] = {
    def exec(nodes: List[FlowGraphNode], result: List[T]): List[T] = nodes match {
      case List() => result
      case _ => exec(nodes.flatMap(node => node.children), nodes.map(node => f(node)) ++ result)
    }

    exec(roots(buildFlowGraph(flowInstance)).toList, Nil)
  }

}

