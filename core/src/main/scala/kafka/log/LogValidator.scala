/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.log

import java.nio.ByteBuffer

import kafka.api.{ApiVersion, KAFKA_2_1_IV0}
import kafka.common.{LongRef, RecordValidationException}
import kafka.message.{CompressionCodec, NoCompressionCodec, ZStdCompressionCodec}
import kafka.server.BrokerTopicStats
import kafka.utils.Logging
import org.apache.kafka.common.errors.{CorruptRecordException, InvalidTimestampException, UnsupportedCompressionTypeException, UnsupportedForMessageFormatException}
import org.apache.kafka.common.record.{AbstractRecords, BufferSupplier, CompressionType, MemoryRecords, Record, RecordBatch, RecordConversionStats, TimestampType}
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.ProduceResponse.RecordError
import org.apache.kafka.common.utils.Time

import scala.collection.{Seq, mutable}
import scala.collection.JavaConverters._

/**
 * The source of an append to the log. This is used when determining required validations.
 */
private[kafka] sealed trait AppendOrigin
private[kafka] object AppendOrigin {

  /**
   * The log append came through replication from the leader. This typically implies minimal validation.
   * Particularly, we do not decompress record batches in order to validate records individually.
   */
  case object Replication extends AppendOrigin

  /**
   * The log append came from either the group coordinator or the transaction coordinator. We validate
   * producer epochs for normal log entries (specifically offset commits from the group coordinator) and
   * we validate coordinate end transaction markers from the transaction coordinator.
   */
  case object Coordinator extends AppendOrigin

  /**
   * The log append came from the client, which implies full validation.
   */
  case object Client extends AppendOrigin
}

private[log] object LogValidator extends Logging {

  /**
   * Update the offsets for this message set and do further validation on messages including:
   * 1. Messages for compacted topics must have keys
   * 2. When magic value >= 1, inner messages of a compressed message set must have monotonically increasing offsets
   *    starting from 0.
   * 3. When magic value >= 1, validate and maybe overwrite timestamps of messages.
   * 4. Declared count of records in DefaultRecordBatch must match number of valid records contained therein.
   *
   * This method will convert messages as necessary to the topic's configured message format version. If no format
   * conversion or value overwriting is required for messages, this method will perform in-place operations to
   * avoid expensive re-compression.
   *
   * Returns a ValidationAndOffsetAssignResult containing the validated message set, maximum timestamp, the offset
   * of the shallow message with the max timestamp and a boolean indicating whether the message sizes may have changed.
   */
  private[log] def validateMessagesAndAssignOffsets(records: MemoryRecords,
                                                    topicPartition: TopicPartition,
                                                    offsetCounter: LongRef,
                                                    time: Time,
                                                    now: Long,
                                                    sourceCodec: CompressionCodec,
                                                    targetCodec: CompressionCodec,
                                                    compactedTopic: Boolean,
                                                    magic: Byte,
                                                    timestampType: TimestampType,
                                                    timestampDiffMaxMs: Long,
                                                    partitionLeaderEpoch: Int,
                                                    origin: AppendOrigin,
                                                    interBrokerProtocolVersion: ApiVersion,
                                                    brokerTopicStats: BrokerTopicStats): ValidationAndOffsetAssignResult = {
    //当producer 设置的压缩类型和 broker服务器配置的压缩类型都是未压缩时
    if (sourceCodec == NoCompressionCodec && targetCodec == NoCompressionCodec) {
      // check the magic value
      //检查records中的magic 值，是否和服务器配置的magic值相等。
      if (!records.hasMatchingMagic(magic))
      //不等的话，就进行消息类型的转换和offset值的分配。
        convertAndAssignOffsetsNonCompressed(records, topicPartition, offsetCounter, compactedTopic, time, now, timestampType,
          timestampDiffMaxMs, magic, partitionLeaderEpoch, origin, brokerTopicStats)
      else
        // Do in-place validation, offset assignment and maybe set timestamp
        //相等的话，做就地的验证，和offset的分配，然后有可能会添加timstamp字段的值。
        assignOffsetsNonCompressed(records, topicPartition, offsetCounter, now, compactedTopic, timestampType, timestampDiffMaxMs,
          partitionLeaderEpoch, origin, magic, brokerTopicStats)
    } else {
      //若是两者都有压缩，或者两者任一有压缩时，调用同功能，可处理压缩类型的处理方法。
      validateMessagesAndAssignOffsetsCompressed(records, topicPartition, offsetCounter, time, now, sourceCodec, targetCodec, compactedTopic,
        magic, timestampType, timestampDiffMaxMs, partitionLeaderEpoch, origin, interBrokerProtocolVersion, brokerTopicStats)
    }
  }

  private def getFirstBatchAndMaybeValidateNoMoreBatches(records: MemoryRecords, sourceCodec: CompressionCodec): RecordBatch = {
    val batchIterator = records.batches.iterator

    if (!batchIterator.hasNext) {
      throw new InvalidRecordException("Record batch has no batches at all")
    }

    val batch = batchIterator.next()

    // if the format is v2 and beyond, or if the messages are compressed, we should check there's only one batch.
    if (batch.magic() >= RecordBatch.MAGIC_VALUE_V2 || sourceCodec != NoCompressionCodec) {
      if (batchIterator.hasNext) {
        throw new InvalidRecordException("Compressed outer record has more than one batch")
      }
    }

    batch
  }

  private def validateBatch(topicPartition: TopicPartition,
                            firstBatch: RecordBatch,
                            batch: RecordBatch,
                            origin: AppendOrigin,
                            toMagic: Byte,
                            brokerTopicStats: BrokerTopicStats): Unit = {
    // batch magic byte should have the same magic as the first batch
    if (firstBatch.magic() != batch.magic()) {
      brokerTopicStats.allTopicsStats.invalidMagicNumberRecordsPerSec.mark()
      throw new InvalidRecordException(s"Batch magic ${batch.magic()} is not the same as the first batch'es magic byte ${firstBatch.magic()} in topic partition $topicPartition.")
    }

    if (origin == AppendOrigin.Client) {
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2) {
        val countFromOffsets = batch.lastOffset - batch.baseOffset + 1
        if (countFromOffsets <= 0) {
          brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
          throw new InvalidRecordException(s"Batch has an invalid offset range: [${batch.baseOffset}, ${batch.lastOffset}] in topic partition $topicPartition.")
        }

        // v2 and above messages always have a non-null count
        val count = batch.countOrNull
        if (count <= 0) {
          brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
          throw new InvalidRecordException(s"Invalid reported count for record batch: $count in topic partition $topicPartition.")
        }

        if (countFromOffsets != batch.countOrNull) {
          brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
          throw new InvalidRecordException(s"Inconsistent batch offset range [${batch.baseOffset}, ${batch.lastOffset}] " +
            s"and count of records $count in topic partition $topicPartition.")
        }
      }

      if (batch.isControlBatch) {
        brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
        throw new InvalidRecordException(s"Clients are not allowed to write control records in topic partition $topicPartition.")
      }

      if (batch.hasProducerId && batch.baseSequence < 0) {
        brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
        throw new InvalidRecordException(s"Invalid sequence number ${batch.baseSequence} in record batch " +
          s"with producerId ${batch.producerId} in topic partition $topicPartition.")
      }
    }

    if (batch.isTransactional && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Transactional records cannot be used with magic version $toMagic")

    if (batch.hasProducerId && toMagic < RecordBatch.MAGIC_VALUE_V2)
      throw new UnsupportedForMessageFormatException(s"Idempotent records cannot be used with magic version $toMagic")
  }

  private def validateRecord(batch: RecordBatch, topicPartition: TopicPartition, record: Record, batchIndex: Int, now: Long,
                             timestampType: TimestampType, timestampDiffMaxMs: Long, compactedTopic: Boolean,
                             brokerTopicStats: BrokerTopicStats): Unit = {
    if (!record.hasMagic(batch.magic)) {
      brokerTopicStats.allTopicsStats.invalidMagicNumberRecordsPerSec.mark()
      throw new RecordValidationException(
        new InvalidRecordException(s"Log record $record's magic does not match outer magic ${batch.magic} in topic partition $topicPartition."),
        List(new RecordError(batchIndex)))
    }

    // verify the record-level CRC only if this is one of the deep entries of a compressed message
    // set for magic v0 and v1. For non-compressed messages, there is no inner record for magic v0 and v1,
    // so we depend on the batch-level CRC check in Log.analyzeAndValidateRecords(). For magic v2 and above,
    // there is no record-level CRC to check.
    if (batch.magic <= RecordBatch.MAGIC_VALUE_V1 && batch.isCompressed) {
      try {
        record.ensureValid()
      } catch {
        case e: InvalidRecordException =>
          brokerTopicStats.allTopicsStats.invalidMessageCrcRecordsPerSec.mark()
          throw new CorruptRecordException(e.getMessage + s" in topic partition $topicPartition.")
      }
    }

    validateKey(record, batchIndex, topicPartition, compactedTopic, brokerTopicStats)
    validateTimestamp(batch, record, batchIndex, now, timestampType, timestampDiffMaxMs)
  }

  private def convertAndAssignOffsetsNonCompressed(records: MemoryRecords,
                                                   topicPartition: TopicPartition,
                                                   offsetCounter: LongRef,
                                                   compactedTopic: Boolean,
                                                   time: Time,
                                                   now: Long,
                                                   timestampType: TimestampType,
                                                   timestampDiffMaxMs: Long,
                                                   toMagicValue: Byte,
                                                   partitionLeaderEpoch: Int,
                                                   origin: AppendOrigin,
                                                   brokerTopicStats: BrokerTopicStats): ValidationAndOffsetAssignResult = {
    val startNanos = time.nanoseconds
    // 评估转换消息类型后的，消息大小。
    val sizeInBytesAfterConversion = AbstractRecords.estimateSizeInBytes(toMagicValue, offsetCounter.value,
      CompressionType.NONE, records.records)
    //属于同一个MessageSet的Batch的producer信息，肯定都是相同的，所以取第一个Batch的producer信息就好。
    val (producerId, producerEpoch, sequence, isTransactional) = {
      val first = records.batches.asScala.head
      (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
    }

    val newBuffer = ByteBuffer.allocate(sizeInBytesAfterConversion)
    //通过MemoryRecords.builder 将不同message_format version 的消息，全部转换为MemoryRecordsBuilder对象。
    val builder = MemoryRecords.builder(newBuffer, toMagicValue, CompressionType.NONE, times tampType,
      offsetCounter.value, now, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch)

    val firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, NoCompressionCodec)
    //迭代遍历每个Batch，对每个Batch进行验证
    for (batch <- records.batches.asScala) {
      validateBatch(topicPartition, firstBatch, batch, origin, toMagicValue, brokerTopicStats)
      //迭代遍历每条Record，对每条Record进行验证，并分配Offset，然后通过builder 写入到builder 中的buffer中去。
      for ((record, batchIndex) <- batch.asScala.view.zipWithIndex) {
        validateRecord(batch, topicPartition, record, batchIndex, now, timestampType, timestampDiffMaxMs, compactedTopic, brokerTopicStats)
        builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
      }
    }
    //返回builder中的已经转换的Records
    val convertedRecords = builder.build()

    val info = builder.info
    val recordConversionStats = new RecordConversionStats(builder.uncompressedBytesWritten,
      builder.numRecords, time.nanoseconds - startNanos)
    ValidationAndOffsetAssignResult(
      validatedRecords = convertedRecords,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordConversionStats = recordConversionStats)
  }

  private def assignOffsetsNonCompressed(records: MemoryRecords,
                                         topicPartition: TopicPartition,
                                         offsetCounter: LongRef,
                                         now: Long,
                                         compactedTopic: Boolean,
                                         timestampType: TimestampType,
                                         timestampDiffMaxMs: Long,
                                         partitionLeaderEpoch: Int,
                                         origin: AppendOrigin,
                                         magic: Byte,
                                         brokerTopicStats: BrokerTopicStats): ValidationAndOffsetAssignResult = {
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    var offsetOfMaxTimestamp = -1L
    //一个request的最初的Offset值：可能包含多个batch
    val initialOffset = offsetCounter.value
    //获取第一个batch
    val firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, NoCompressionCodec)
    //
    for (batch <- records.batches.asScala) {
      //验证Batch信息
      validateBatch(topicPartition, firstBatch, batch, origin, magic, brokerTopicStats)

      var maxBatchTimestamp = RecordBatch.NO_TIMESTAMP
      var offsetOfMaxBatchTimestamp = -1L
      //遍历batch中的每一条record
      for ((record, batchIndex) <- batch.asScala.view.zipWithIndex) {
        validateRecord(batch, topicPartition, record, batchIndex, now, timestampType, timestampDiffMaxMs, compactedTopic, brokerTopicStats)
        //分配offset
        val offset = offsetCounter.getAndIncrement()
        //非vo版本的话，需要记录Timastamp 相关属性
        if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && record.timestamp > maxBatchTimestamp) {
          maxBatchTimestamp = record.timestamp
          offsetOfMaxBatchTimestamp = offset
        }
      }
      //非v0版本，记录当前request中的最大时间戳，以及响应的offset
      if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && maxBatchTimestamp > maxTimestamp) {
        maxTimestamp = maxBatchTimestamp
        offsetOfMaxTimestamp = offsetOfMaxBatchTimestamp
      }
      //给当前batch最后的offset
      batch.setLastOffset(offsetCounter.value - 1)
      //设置magic=v2的 partitionLeaderEpoch属性
      if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
        batch.setPartitionLeaderEpoch(partitionLeaderEpoch)
      //在magic的v0和v1版本中，如果timestampType=LOG_APPEND_TIME,那么batch 中时间戳设置为LOG_APPEND_TIME, 时间设置为now
      //如果timestampType=CREATE_TIME,那么Batch中的时间戳为CREATE_TIME,time 为当前batch最大时间戳。
      if (batch.magic > RecordBatch.MAGIC_VALUE_V0) {
        if (timestampType == TimestampType.LOG_APPEND_TIME)
          batch.setMaxTimestamp(TimestampType.LOG_APPEND_TIME, now)
        else
          batch.setMaxTimestamp(timestampType, maxBatchTimestamp)
      }
    }
    //如果timestampType 为LOG_APPEND_TIME,那么 当前request 最大时间戳为 now，
    //如果是V2版本的话，那么当前最大时间戳的offset 是当前request最后一个offset，
    //v0和v1版本的话，那么是当前request 中的第一个offset。
    if (timestampType == TimestampType.LOG_APPEND_TIME) {
      maxTimestamp = now
      if (magic >= RecordBatch.MAGIC_VALUE_V2)
        offsetOfMaxTimestamp = offsetCounter.value - 1
      else
        offsetOfMaxTimestamp = initialOffset
    }

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = maxTimestamp,
      shallowOffsetOfMaxTimestamp = offsetOfMaxTimestamp,
      messageSizeMaybeChanged = false,
      recordConversionStats = RecordConversionStats.EMPTY)
  }

  /**
   * We cannot do in place assignment in one of the following situations:
   * 1. Source and target compression codec are different
   * 2. When the target magic is not equal to batches' magic, meaning format conversion is needed.
   * 3. When the target magic is equal to V0, meaning absolute offsets need to be re-assigned.
   */
  def validateMessagesAndAssignOffsetsCompressed(records: MemoryRecords,
                                                 topicPartition: TopicPartition,
                                                 offsetCounter: LongRef,
                                                 time: Time,
                                                 now: Long,
                                                 sourceCodec: CompressionCodec,
                                                 targetCodec: CompressionCodec,
                                                 compactedTopic: Boolean,
                                                 toMagic: Byte,
                                                 timestampType: TimestampType,
                                                 timestampDiffMaxMs: Long,
                                                 partitionLeaderEpoch: Int,
                                                 origin: AppendOrigin,
                                                 interBrokerProtocolVersion: ApiVersion,
                                                 brokerTopicStats: BrokerTopicStats): ValidationAndOffsetAssignResult = {
    //kafka2.1版本以下，不能设置ZSTD压缩方法。
    if (targetCodec == ZStdCompressionCodec && interBrokerProtocolVersion < KAFKA_2_1_IV0)
      throw new UnsupportedCompressionTypeException("Produce requests to inter.broker.protocol.version < 2.1 broker " +
        "are not allowed to use ZStandard compression")

    //当producer和 broker设置压缩类型一致时，可以就地分配
    // No in place assignment situation 1
    var inPlaceAssignment = sourceCodec == targetCodec
    //默认最大时间戳为-1
    var maxTimestamp = RecordBatch.NO_TIMESTAMP
    //检验内部Offset 是否从0开始一次递增
    val expectedInnerOffset = new LongRef(0)
    //存储通过检验当Record
    val validatedRecords = new mutable.ArrayBuffer[Record]
    //存储解压后总Record Bytes大小
    var uncompressedSizeInBytes = 0

    // Assume there's only one batch with compressed memory records; otherwise, return InvalidRecordException
    // One exception though is that with format smaller than v2, if sourceCodec is noCompression, then each batch is actually
    // a single record so we'd need to special handle it by creating a single wrapper batch that includes all the records
    //默认一个Request是只能包含一个Batch的，如果包含多个就报InvalidRecordException
    //其中有例外情况就是 message_format version 是小于v2的，在v1/v0版本中，如果producer设置未压缩状态的话，一个Batch是只包含一条Record的，因此
    //我们需要特别处理这种情况，通过创建一个Wrapper Batch 来包含所有的 record
    val firstBatch = getFirstBatchAndMaybeValidateNoMoreBatches(records, sourceCodec)

    //当producer的magic版本和broker的magic版本是不一样的话，或者是虽然两者的magic版本一样，但是是v0版本的时候
    //也不能就地转换。
    // No in place assignment situation 2 and 3: we only need to check for the first batch because:
    //  1. For most cases (compressed records, v2, for example), there's only one batch anyways.
    //  2. For cases that there may be multiple batches, all batches' magic should be the same.
    if (firstBatch.magic != toMagic || toMagic == RecordBatch.MAGIC_VALUE_V0)
      inPlaceAssignment = false
    //不压缩control records ，除非在batch中写了要进行压缩
    // Do not compress control records unless they are written compressed
    if (sourceCodec == NoCompressionCodec && firstBatch.isControlBatch)
      inPlaceAssignment = true
    //
    val batches = records.batches.asScala
    for (batch <- batches) {
      validateBatch(topicPartition, firstBatch, batch, origin, toMagic, brokerTopicStats)
      uncompressedSizeInBytes += AbstractRecords.recordBatchHeaderSizeInBytes(toMagic, batch.compressionType())

      // if we are on version 2 and beyond, and we know we are going for in place assignment,
      // then we can optimize the iterator to skip key / value / headers since they would not be used at all
      //如果就Batch中magic的版本是>=2的，并且可以就地转换的话，在进行Record 解析的时候，可以不解析key/value/headers
      val recordsIterator = if (inPlaceAssignment && firstBatch.magic >= RecordBatch.MAGIC_VALUE_V2)
        batch.skipKeyValueIterator(BufferSupplier.NO_CACHING)
      else
        batch.streamingIterator(BufferSupplier.NO_CACHING)

      try {
        //利用batch中的迭代器解析成record
        for ((record, batchIndex) <- batch.asScala.view.zipWithIndex) {
          if (sourceCodec != NoCompressionCodec && record.isCompressed)
            throw new RecordValidationException(
              new InvalidRecordException(s"Compressed outer record should not have an inner record with a compression attribute set: $record"),
              List(new RecordError(batchIndex)))

          validateRecord(batch, topicPartition, record, batchIndex, now, timestampType, timestampDiffMaxMs, compactedTopic, brokerTopicStats)
          //统计未压缩的record尺寸
          uncompressedSizeInBytes += record.sizeInBytes()
          //如果两者的magic 都大于0的话，说明Record内部都是 Offset Deleta ，需要进行验证。
          if (batch.magic > RecordBatch.MAGIC_VALUE_V0 && toMagic > RecordBatch.MAGIC_VALUE_V0) {
            // inner records offset should always be continuous
            val expectedOffset = expectedInnerOffset.getAndIncrement()
            //不相等的报错
            if (record.offset != expectedOffset) {
              brokerTopicStats.allTopicsStats.invalidOffsetOrSequenceRecordsPerSec.mark()
              throw new RecordValidationException(
                new InvalidRecordException(s"Inner record $record inside the compressed record batch does not have incremental offsets, expected offset is $expectedOffset in topic partition $topicPartition."),
                List(new RecordError(batchIndex)))
            }
            //记录最大TimesTamp
            if (record.timestamp > maxTimestamp)
              maxTimestamp = record.timestamp
          }

          validatedRecords += record
        }
      } finally {
        recordsIterator.close()
      }
    }

    if (!inPlaceAssignment) {
      //如果不能就地转换的话，需要利用MemoryBuilder进行message_format_version的格式转换，
      //或者压缩类型转换，这里可以发现，不管是格式转换，还是压缩类型转换，耗费的资源是一样的。
      //都需要重新build 目的版本的magic，并且进行重新的压缩。
      val (producerId, producerEpoch, sequence, isTransactional) = {
        // note that we only reassign offsets for requests coming straight from a producer. For records with magic V2,
        // there should be exactly one RecordBatch per request, so the following is all we need to do. For Records
        // with older magic versions, there will never be a producer id, etc.
        val first = records.batches.asScala.head
        (first.producerId, first.producerEpoch, first.baseSequence, first.isTransactional)
      }
      buildRecordsAndAssignOffsets(toMagic, offsetCounter, time, timestampType, CompressionType.forId(targetCodec.codec),
        now, validatedRecords, producerId, producerEpoch, sequence, isTransactional, partitionLeaderEpoch,
        uncompressedSizeInBytes)
    } else {
      //如果可以就地转换的话，只需要在BatchHeader中填充对应的lastOffset 信息，maxTimestamp信息，partitionLeader信息，即可。
      // we can update the batch only and write the compressed payload as is;
      // again we assume only one record batch within the compressed set
      val batch = records.batches.iterator.next()
      val lastOffset = offsetCounter.addAndGet(validatedRecords.size) - 1
      //lastOffset信息
      batch.setLastOffset(lastOffset)
      //maxTimestamp信息
      if (timestampType == TimestampType.LOG_APPEND_TIME)
        maxTimestamp = now

      if (toMagic >= RecordBatch.MAGIC_VALUE_V1)
        batch.setMaxTimestamp(timestampType, maxTimestamp)
      //partition leader信息
      if (toMagic >= RecordBatch.MAGIC_VALUE_V2)
        batch.setPartitionLeaderEpoch(partitionLeaderEpoch)

      val recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes, 0, 0)
      ValidationAndOffsetAssignResult(validatedRecords = records,
        maxTimestamp = maxTimestamp,
        shallowOffsetOfMaxTimestamp = lastOffset,
        messageSizeMaybeChanged = false,
        recordConversionStats = recordConversionStats)
    }
  }

  private def buildRecordsAndAssignOffsets(magic: Byte,
                                           offsetCounter: LongRef,
                                           time: Time,
                                           timestampType: TimestampType,
                                           compressionType: CompressionType,
                                           logAppendTime: Long,
                                           validatedRecords: Seq[Record],
                                           producerId: Long,
                                           producerEpoch: Short,
                                           baseSequence: Int,
                                           isTransactional: Boolean,
                                           partitionLeaderEpoch: Int,
                                           uncompressedSizeInBytes: Int): ValidationAndOffsetAssignResult = {
    val startNanos = time.nanoseconds
    val estimatedSize = AbstractRecords.estimateSizeInBytes(magic, offsetCounter.value, compressionType,
      validatedRecords.asJava)
    val buffer = ByteBuffer.allocate(estimatedSize)
    val builder = MemoryRecords.builder(buffer, magic, compressionType, timestampType, offsetCounter.value,
      logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, partitionLeaderEpoch)

    validatedRecords.foreach { record =>
      builder.appendWithOffset(offsetCounter.getAndIncrement(), record)
    }

    val records = builder.build()

    val info = builder.info

    // This is not strictly correct, it represents the number of records where in-place assignment is not possible
    // instead of the number of records that were converted. It will over-count cases where the source and target are
    // message format V0 or if the inner offsets are not consecutive. This is OK since the impact is the same: we have
    // to rebuild the records (including recompression if enabled).
    val conversionCount = builder.numRecords
    val recordConversionStats = new RecordConversionStats(uncompressedSizeInBytes + builder.uncompressedBytesWritten,
      conversionCount, time.nanoseconds - startNanos)

    ValidationAndOffsetAssignResult(
      validatedRecords = records,
      maxTimestamp = info.maxTimestamp,
      shallowOffsetOfMaxTimestamp = info.shallowOffsetOfMaxTimestamp,
      messageSizeMaybeChanged = true,
      recordConversionStats = recordConversionStats)
  }

  private def validateKey(record: Record, batchIndex: Int, topicPartition: TopicPartition, compactedTopic: Boolean, brokerTopicStats: BrokerTopicStats) {
    if (compactedTopic && !record.hasKey) {
      brokerTopicStats.allTopicsStats.noKeyCompactedTopicRecordsPerSec.mark()
      throw new RecordValidationException(
        new InvalidRecordException(s"Compacted topic cannot accept message without key in topic partition $topicPartition."),
        List(new RecordError(batchIndex)))
    }
  }

  /**
   * This method validates the timestamps of a message.
   * If the message is using create time, this method checks if it is within acceptable range.
   */
  private def validateTimestamp(batch: RecordBatch,
                                record: Record,
                                batchIndex: Int,
                                now: Long,
                                timestampType: TimestampType,
                                timestampDiffMaxMs: Long): Unit = {
    if (timestampType == TimestampType.CREATE_TIME
      && record.timestamp != RecordBatch.NO_TIMESTAMP
      && math.abs(record.timestamp - now) > timestampDiffMaxMs)
      throw new RecordValidationException(
        new InvalidTimestampException(s"Timestamp ${record.timestamp} of message with offset ${record.offset} is " +
          s"out of range. The timestamp should be within [${now - timestampDiffMaxMs}, ${now + timestampDiffMaxMs}]"),
        List(new RecordError(batchIndex)))
    if (batch.timestampType == TimestampType.LOG_APPEND_TIME)
      throw new RecordValidationException(
        new InvalidTimestampException(s"Invalid timestamp type in message $record. Producer should not set " +
          s"timestamp type to LogAppendTime."),
        List(new RecordError(batchIndex)))
  }

  case class ValidationAndOffsetAssignResult(validatedRecords: MemoryRecords,
                                             maxTimestamp: Long,
                                             shallowOffsetOfMaxTimestamp: Long,
                                             messageSizeMaybeChanged: Boolean,
                                             recordConversionStats: RecordConversionStats)

}
