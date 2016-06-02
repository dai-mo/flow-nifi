package org.dcs.flow.model

/**
  * Created by cmathew on 30/05/16.
  */
class FlowInstance {
  var id: String = _
  var version: String = _
  var processors : List[ProcessorInstance] = _
  var connections: List[Connection] = _
}
