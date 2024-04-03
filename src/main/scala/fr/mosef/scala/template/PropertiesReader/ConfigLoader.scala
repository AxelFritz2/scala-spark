package fr.mosef.scala.template.PropertiesReader

import java.util.Properties
import scala.util.Using

object ConfigLoader {
  private val properties: Properties = new Properties()

  loadProperties()

  def loadProperties(): Unit = {
    Using(getClass.getClassLoader.getResourceAsStream("application.properties")) { resourceStream =>
      if (resourceStream == null) {
        throw new IllegalStateException("Fichier application.properties introuvable dans les ressources.")
      }
      properties.load(resourceStream)
    } recover {
      case e: Exception => e.printStackTrace()
    }
  }

  def getProperty(key: String): Option[String] = Option(properties.getProperty(key))
}
