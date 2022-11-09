package zerolog

import (
	"time"

	"github.com/rs/zerolog"

	"github.com/ydb-platform/ydb-go-sdk/v3/trace"
)

func Topic(logger *zerolog.Logger, details trace.Details, opts ...option) trace.Topic {
	scope := "ydb.topic"
	t := trace.Topic{}

	///
	/// Topic reader
	///
	if details&trace.TopicReaderStreamLifeCycleEvents != 0 {
		scope := scope + ".reader.lifecycle"

		t.OnReaderReconnect = func(startInfo trace.TopicReaderReconnectStartInfo) func(doneInfo trace.TopicReaderReconnectDoneInfo) {
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Msg("reconnecting")

			start := time.Now()
			return func(doneInfo trace.TopicReaderReconnectDoneInfo) {
				logger.Info().Caller().Timestamp().Str("scope", scope).
					Err(doneInfo.Error).
					Dur("latency", time.Since(start)).
					Msg("reconnected")
			}
		}

		t.OnReaderReconnectRequest = func(info trace.TopicReaderReconnectRequestInfo) {
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				AnErr("reason", info.Reason).
				Bool("was_sent", info.WasSent).
				Msg("request reconnect")
		}
	}
	if details&trace.TopicReaderPartitionEvents != 0 {
		scope := scope + ".reader.partition"
		t.OnReaderPartitionReadStartResponse = func(startInfo trace.TopicReaderPartitionReadStartResponseStartInfo) func(stopInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) { //nolint:lll
			start := time.Now()

			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("topic", startInfo.Topic).
				Str("reader_connection_id", startInfo.ReaderConnectionID).
				Int64("partition_id", startInfo.PartitionID).
				Int64("partition_session_id", startInfo.PartitionSessionID).
				Msg("read partition response starting...")

			return func(doneInfo trace.TopicReaderPartitionReadStartResponseDoneInfo) {
				logger.Debug().Caller().Timestamp().Str("scope", scope).
					Err(doneInfo.Error).
					Str("topic", startInfo.Topic).
					Str("reader_connection_id", startInfo.ReaderConnectionID).
					Int64("partition_id", startInfo.PartitionID).
					Int64("partition_session_id", startInfo.PartitionSessionID).
					//
					Dur("latency", time.Since(start)).
					Interface("commit_offset", doneInfo.CommitOffset).
					Interface("read_offset", doneInfo.ReadOffset).
					Msg("read partition response completed")
			}
		}

		t.OnReaderPartitionReadStopResponse = func(startInfo trace.TopicReaderPartitionReadStopResponseStartInfo) func(trace.TopicReaderPartitionReadStopResponseDoneInfo) {
			start := time.Now()
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("reader_connection_id", startInfo.ReaderConnectionID).
				Str("topic", startInfo.Topic).
				Int64("partition_id", startInfo.PartitionID).
				Int64("partition_session_id", startInfo.PartitionSessionID).
				Int64("commit_offset", startInfo.CommittedOffset).
				Bool("graceful", startInfo.Graceful).
				Msg("reader partition stopping...")

			return func(doneInfo trace.TopicReaderPartitionReadStopResponseDoneInfo) {
				logInfoWarn(logger, doneInfo.Error).Caller().Timestamp().Str("scope", scope).
					Str("reader_connection_id", startInfo.ReaderConnectionID).
					Str("topic", startInfo.Topic).
					Int64("partition_id", startInfo.PartitionID).
					Int64("partition_session_id", startInfo.PartitionSessionID).
					Int64("commit_offset", startInfo.CommittedOffset).
					Bool("graceful", startInfo.Graceful).
					//
					Dur("latency", time.Since(start)).
					Msg("reader partition stopped")
			}
		}
	}

	if details&trace.TopicReaderStreamEvents != 0 {
		scope := scope + ".reader.stream"

		t.OnReaderCommit = func(startInfo trace.TopicReaderCommitStartInfo) func(doneInfo trace.TopicReaderCommitDoneInfo) {
			start := time.Now()

			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("topic", startInfo.Topic).
				Int64("partition_id", startInfo.PartitionID).
				Int64("partition_session_id", startInfo.PartitionSessionID).
				Int64("commit_start_offset", startInfo.StartOffset).
				Int64("commit_end_offset", startInfo.EndOffset).
				Msg("start committing...")

			return func(doneInfo trace.TopicReaderCommitDoneInfo) {
				logDebugWarn(logger, doneInfo.Error).Caller().Timestamp().Str("scope", scope).
					Str("topic", startInfo.Topic).
					Int64("partition_id", startInfo.PartitionID).
					Int64("partition_session_id", startInfo.PartitionSessionID).
					Int64("commit_start_offset", startInfo.StartOffset).
					Int64("commit_end_offset", startInfo.EndOffset).
					//
					Dur("latency", time.Since(start)).
					Msg("committed")
			}
		}

		t.OnReaderSendCommitMessage = func(startInfo trace.TopicReaderSendCommitMessageStartInfo) func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
			start := time.Now()

			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Ints64("partition_ids", startInfo.CommitsInfo.PartitionIDs()).
				Ints64("partition_session_ids", startInfo.CommitsInfo.PartitionSessionIDs()).
				Msg("commit message sending...")

			return func(doneInfo trace.TopicReaderSendCommitMessageDoneInfo) {
				logDebugWarn(logger, doneInfo.Error).Caller().Timestamp().Str("scope", scope).
					Ints64("partition_ids", startInfo.CommitsInfo.PartitionIDs()).
					Ints64("partition_session_ids", startInfo.CommitsInfo.PartitionSessionIDs()).
					//
					Dur("latency", time.Since(start)).
					Msg("commit message sent")
			}
		}

		t.OnReaderCommittedNotify = func(info trace.TopicReaderCommittedNotifyInfo) {
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("reader_connection_id", info.ReaderConnectionID).
				Str("topic", info.Topic).
				Int64("partition_id", info.PartitionID).
				Int64("partition_session_id", info.PartitionSessionID).
				Int64("committed_offset", info.CommittedOffset).
				Msg("commit ack")
		}

		t.OnReaderClose = func(startInfo trace.TopicReaderCloseStartInfo) func(doneInfo trace.TopicReaderCloseDoneInfo) {
			start := time.Now()

			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("reader_connection_id", startInfo.ReaderConnectionID).
				Str("close_reason", startInfo.CloseReason.Error()).
				Msg("stream closing...")

			return func(doneInfo trace.TopicReaderCloseDoneInfo) {
				logDebugWarn(logger, doneInfo.CloseError).Caller().Timestamp().Str("scope", scope).
					Str("reader_connection_id", startInfo.ReaderConnectionID).
					Str("close_reason", startInfo.CloseReason.Error()).
					//
					Dur("latency", time.Since(start)).
					Msg("topic reader stream closed")
			}
		}

		t.OnReaderInit = func(startInfo trace.TopicReaderInitStartInfo) func(doneInfo trace.TopicReaderInitDoneInfo) {
			start := time.Now()
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID).
				Str("consumer", startInfo.InitRequestInfo.GetConsumer()).
				Strs("topics", startInfo.InitRequestInfo.GetTopics()).
				Msg("stream init starting...")

			return func(doneInfo trace.TopicReaderInitDoneInfo) {
				logDebugWarn(logger, doneInfo.Error).Caller().Timestamp().Str("scope", scope).
					Str("pre_init_reader_connection_id", startInfo.PreInitReaderConnectionID).
					Str("consumer", startInfo.InitRequestInfo.GetConsumer()).
					Strs("topics", startInfo.InitRequestInfo.GetTopics()).
					//
					Dur("latency", time.Since(start)).
					Msg("topic reader stream initialized")
			}
		}

		t.OnReaderError = func(info trace.TopicReaderErrorInfo) {
			logger.Warn().Caller().Timestamp().Str("scope", scope).
				Err(info.Error).
				Str("reader_connection_id", info.ReaderConnectionID).
				Msg("stream error")
		}

		t.OnReaderUpdateToken = func(startInfo trace.OnReadUpdateTokenStartInfo) func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
			start := time.Now()
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("reader_connection_id", startInfo.ReaderConnectionID).
				Msg("token updating...")

			return func(updateTokenInfo trace.OnReadUpdateTokenMiddleTokenReceivedInfo) func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
				logDebugWarn(logger, updateTokenInfo.Error).Caller().Timestamp().Str("scope", scope).
					Str("reader_connection_id", startInfo.ReaderConnectionID).
					//
					Dur("latency", time.Since(start)).
					Int("token_len", updateTokenInfo.TokenLen).
					Msg("got token")

				return func(doneInfo trace.OnReadStreamUpdateTokenDoneInfo) {
					logDebugWarn(logger, doneInfo.Error).Caller().Timestamp().Str("scope", scope).
						Str("reader_connection_id", startInfo.ReaderConnectionID).
						//
						Int("token_len", updateTokenInfo.TokenLen).
						//
						Dur("latency", time.Since(start)).
						Msg("token updated on stream")
				}
			}
		}
	}

	if details&trace.TopicReaderMessageEvents != 0 {
		scope := scope + ".reader.message"

		t.OnReaderSentDataRequest = func(info trace.TopicReaderSentDataRequestInfo) {
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("reader_connection_id", info.ReaderConnectionID).
				Int("request_bytes", info.RequestBytes).
				Int("local_capacity", info.LocalBufferSizeAfterSent).
				Msg("sent data request")
		}

		t.OnReaderReceiveDataResponse = func(startInfo trace.TopicReaderReceiveDataResponseStartInfo) func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
			start := time.Now()
			partitionsCount, batchesCount, messagesCount := startInfo.DataResponse.GetPartitionBatchMessagesCounts()

			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("reader_connection_id", startInfo.ReaderConnectionID).
				Int("received_bytes", startInfo.DataResponse.GetBytesSize()).
				Int("local_capacity", startInfo.LocalBufferSizeAfterReceive).
				Int("partitions_count", partitionsCount).
				Int("batches_count", batchesCount).
				Int("messages_count", messagesCount).
				Msg("data response received, process starting...")

			return func(doneInfo trace.TopicReaderReceiveDataResponseDoneInfo) {
				logDebugWarn(logger, doneInfo.Error).
					Str("reader_connection_id", startInfo.ReaderConnectionID).
					Int("received_bytes", startInfo.DataResponse.GetBytesSize()).
					Int("local_capacity", startInfo.LocalBufferSizeAfterReceive).
					Int("partitions_count", partitionsCount).
					Int("batches_count", batchesCount).
					Int("messages_count", messagesCount).
					//
					Dur("latency", time.Since(start)).
					Msg("data response received and processed")
			}
		}

		t.OnReaderReadMessages = func(startInfo trace.TopicReaderReadMessagesStartInfo) func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
			start := time.Now()
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Int("min_count", startInfo.MinCount).
				Int("max_count", startInfo.MaxCount).
				Int("local_capacity_before", startInfo.FreeBufferCapacity).
				Msg("read messages called, waiting...")

			return func(doneInfo trace.TopicReaderReadMessagesDoneInfo) {
				logDebugInfo(logger, doneInfo.Error).
					Int("min_count", startInfo.MinCount).
					Int("max_count", startInfo.MaxCount).
					Int("local_capacity_before", startInfo.FreeBufferCapacity).
					//
					Str("topic", doneInfo.Topic).
					Int64("partition_id", doneInfo.PartitionID).
					Int("messages_count", doneInfo.MessagesCount).
					Int("local_capacity_after", doneInfo.FreeBufferCapacity).
					Dur("latency", time.Since(start)).
					Msg("read messages returned")
			}
		}

		t.OnReaderUnknownGrpcMessage = func(info trace.OnReadUnknownGrpcMessageInfo) {
			logger.Info().Caller().Timestamp().Str("scope", scope).
				Err(info.Error).
				Str("reader_connection_id", info.ReaderConnectionID).
				Msg("received unknown message")
		}
	}

	///
	/// Topic writer
	///
	if details&trace.TopicWriterStreamLifeCycleEvents != 0 {
		scope := scope + ".writer.lifecycle"
		t.OnWriterReconnect = func(startInfo trace.TopicWriterReconnectStartInfo) func(doneInfo trace.TopicWriterReconnectDoneInfo) {
			start := time.Now()
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("topic", startInfo.Topic).
				Str("producer_id", startInfo.ProducerID).
				Str("writer_instance_id", startInfo.WriterInstanceID).
				Int("attempt", startInfo.Attempt).
				Msg("connect to topic writer stream starting...")

			return func(doneInfo trace.TopicWriterReconnectDoneInfo) {
				logDebugInfo(logger, doneInfo.Error).
					Str("topic", startInfo.Topic).
					Str("producer_id", startInfo.ProducerID).
					Str("writer_instance_id", startInfo.WriterInstanceID).
					Int("attempt", startInfo.Attempt).
					//
					Dur("latency", time.Since(start)).
					Msg("connect to topic writer stream completed")
			}
		}
		t.OnWriterInitStream = func(startInfo trace.TopicWriterInitStreamStartInfo) func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
			start := time.Now()
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("topic", startInfo.Topic).
				Str("producer_id", startInfo.ProducerID).
				Str("writer_instance_id", startInfo.WriterInstanceID).
				Msg("init stream starting...")

			return func(doneInfo trace.TopicWriterInitStreamDoneInfo) {
				logDebugInfo(logger, doneInfo.Error).
					Str("topic", startInfo.Topic).
					Str("producer_id", startInfo.ProducerID).
					Str("writer_instance_id", startInfo.WriterInstanceID).
					//
					Dur("latency", time.Since(start)).
					Str("session", doneInfo.SessionID).
					Msg("init stream completed {topic:'%v', producer_id:'%v', writer_instance_id: '%v'")
			}
		}
		t.OnWriterClose = func(startInfo trace.TopicWriterCloseStartInfo) func(doneInfo trace.TopicWriterCloseDoneInfo) {
			start := time.Now()
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("writer_instance_id", startInfo.WriterInstanceID).
				AnErr("reason", startInfo.Reason).
				Msg("close topic writer starting... ")

			return func(doneInfo trace.TopicWriterCloseDoneInfo) {
				logDebugInfo(logger, doneInfo.Error).
					Str("writer_instance_id", startInfo.WriterInstanceID).
					AnErr("reason", startInfo.Reason).
					//
					Dur("latency", time.Since(start)).
					Msg("close topic writer completed")
			}
		}
	}
	if details&trace.TopicWriterStreamEvents != 0 {
		scope := scope + ".writer.stream"
		t.OnWriterCompressMessages = func(startInfo trace.TopicWriterCompressMessagesStartInfo) func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
			start := time.Now()
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("writer_instance_id", startInfo.WriterInstanceID).
				Str("session_id", startInfo.SessionID).
				Stringer("reason", startInfo.Reason).
				Int32("codec", startInfo.Codec).
				Int("messages_count", startInfo.MessagesCount).
				Int64("first_seqno", startInfo.FirstSeqNo).
				Msg("compress message starting...")

			return func(doneInfo trace.TopicWriterCompressMessagesDoneInfo) {
				logDebugInfo(logger, doneInfo.Error).
					Str("writer_instance_id", startInfo.WriterInstanceID).
					Str("session_id", startInfo.SessionID).
					Stringer("reason", startInfo.Reason).
					Int32("codec", startInfo.Codec).
					Int("messages_count", startInfo.MessagesCount).
					Int64("first_seqno", startInfo.FirstSeqNo).
					//
					Dur("latency", time.Since(start)).
					Msg("compress message completed")
			}
		}
		t.OnWriterSendMessages = func(startInfo trace.TopicWriterSendMessagesStartInfo) func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
			start := time.Now()
			logger.Debug().Caller().Timestamp().Str("scope", scope).
				Str("writer_instance_id", startInfo.WriterInstanceID).
				Str("session_id", startInfo.SessionID).
				Int32("codec", startInfo.Codec).
				Int("messages_count", startInfo.MessagesCount).
				Int64("first_seqno", startInfo.FirstSeqNo).
				Msg("compress message starting...")

			return func(doneInfo trace.TopicWriterSendMessagesDoneInfo) {
				logDebugInfo(logger, doneInfo.Error).
					Str("writer_instance_id", startInfo.WriterInstanceID).
					Str("session_id", startInfo.SessionID).
					Int32("codec", startInfo.Codec).
					Int("messages_count", startInfo.MessagesCount).
					Int64("first_seqno", startInfo.FirstSeqNo).
					//
					Dur("latency", time.Since(start)).
					Msg("compress message completed")
			}
		}
		t.OnWriterReadUnknownGrpcMessage = func(info trace.TopicOnWriterReadUnknownGrpcMessageInfo) {
			logger.Info().Caller().Timestamp().Str("scope", scope).
				Str("writer_instance_id", info.WriterInstanceID).
				Str("session_id", info.SessionID).
				Err(info.Error).
				Msg("topic writer receive unknown message from server")
		}
	}

	return t
}
