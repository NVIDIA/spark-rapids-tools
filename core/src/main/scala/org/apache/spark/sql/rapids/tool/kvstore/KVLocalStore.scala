package org.apache.spark.sql.rapids.tool.kvstore

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.status.AppStatusStoreMetadata
import org.apache.spark.status.KVUtils.open
import org.apache.spark.util.kvstore.KVStore

class KVLocalStore extends Logging{

  private val _path = File.createTempFile("rocksdb","test")
  _path.delete()
  private val db: KVStore = open(_path,
    AppStatusStoreMetadata(2L)
    , new SparkConf().set("spark.history.store.hybridStore.diskBackend","ROCKSDB"), live = true)

  def write[T](obj: T): Unit = {
    db.write(obj)
  }

  def read[T](cls:Class[T], key: Any): T = {
    db.read(cls, key)
  }

  def delete[T](cls:Class[T], key: Any): Unit = {
    db.delete(cls, key)
  }
}