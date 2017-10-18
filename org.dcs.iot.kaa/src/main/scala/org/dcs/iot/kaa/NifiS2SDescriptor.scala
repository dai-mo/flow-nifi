package org.dcs.iot.kaa

import org.apache.avro.Schema
import org.kaaproject.kaa.server.common.plugin.{KaaPluginConfig, PluginConfig, PluginType}

/**
  * Descriptor for {@link org.dcs.iot.kaa.NifiS2SAppender} appender.
  *
  * @author cmathew
  * @constructor
  */
@KaaPluginConfig(pluginType = PluginType.LOG_APPENDER)
class NifiS2SDescriptor extends PluginConfig {

  override def getPluginConfigSchema: Schema =
    NifiS2SConfiguration.getClassSchema

  override def getPluginTypeName: String =
    "Nifi S2S Appender"

  override def getPluginClassName: String =
    "org.dcs.iot.kaa.NifiS2SAppender"
}
