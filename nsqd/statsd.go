package nsqd

import (
	"fmt"
	"math"
	"net"
	"time"

	"github.com/nsqio/nsq/internal/statsd"
	"github.com/nsqio/nsq/internal/writers"
)

type Uint64Slice []uint64

func (s Uint64Slice) Len() int {
	return len(s)
}

func (s Uint64Slice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (s Uint64Slice) Less(i, j int) bool {
	return s[i] < s[j]
}
//通过UDP来定期推送数据(消息的统计, 内存消耗等)给statsd_address(配置的地址),可以使用Graphite+Grafana搭建更强大的监控
func (n *NSQD) statsdLoop() {
	var lastMemStats memStats
	var lastStats []TopicStats
	interval := n.getOpts().StatsdInterval // 60s定时器
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-n.exitChan:
			goto exit
		case <-ticker.C: //每隔60s,就统计一次并推送给指定addr。
			addr := n.getOpts().StatsdAddress // 获取推送地址和前缀
			prefix := n.getOpts().StatsdPrefix
			conn, err := net.DialTimeout("udp", addr, time.Second) //注意此处采用udp的方式推送,如果此处的add没有配置正确，那么将会有err产生。
			if err != nil {
				n.logf(LOG_ERROR, "failed to create UDP socket to statsd(%s)", addr)
				continue
			}
			sw := writers.NewSpreadWriter(conn, interval-time.Second, n.exitChan)
			bw := writers.NewBoundaryBufferedWriter(sw, n.getOpts().StatsdUDPPacketSize)  // StatsdUDPPacketSize: 508
			client := statsd.NewClient(bw, prefix)

			n.logf(LOG_INFO, "STATSD: pushing stats to %s", addr)

			stats := n.GetStats("", "", false) // 获取所有的topics stats
			for _, topic := range stats {
				// 找到最后一次连接时的topic
				lastTopic := TopicStats{}
				for _, checkTopic := range lastStats {
					if topic.TopicName == checkTopic.TopicName {
						lastTopic = checkTopic
						break
					}
				}
				diff := topic.MessageCount - lastTopic.MessageCount
				stat := fmt.Sprintf("topic.%s.message_count", topic.TopicName)
				client.Incr(stat, int64(diff))

				diff = topic.MessageBytes - lastTopic.MessageBytes
				stat = fmt.Sprintf("topic.%s.message_bytes", topic.TopicName)
				client.Incr(stat, int64(diff))

				stat = fmt.Sprintf("topic.%s.depth", topic.TopicName)
				client.Gauge(stat, topic.Depth)

				stat = fmt.Sprintf("topic.%s.backend_depth", topic.TopicName)
				client.Gauge(stat, topic.BackendDepth)

				for _, item := range topic.E2eProcessingLatency.Percentiles {
					stat = fmt.Sprintf("topic.%s.e2e_processing_latency_%.0f", topic.TopicName, item["quantile"]*100.0)
					//我们可以将该值转换为int64，因为值1是我们将拥有的最小分辨率，所以不存在精度损失
					client.Gauge(stat, int64(item["value"]))
				}

				for _, channel := range topic.Channels {  // channel 统计信息
					// 找到最后一次连接时的channel
					lastChannel := ChannelStats{}
					for _, checkChannel := range lastTopic.Channels {
						if channel.ChannelName == checkChannel.ChannelName {
							lastChannel = checkChannel
							break
						}
					}
					diff := channel.MessageCount - lastChannel.MessageCount
					stat := fmt.Sprintf("topic.%s.channel.%s.message_count", topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					stat = fmt.Sprintf("topic.%s.channel.%s.depth", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, channel.Depth)

					stat = fmt.Sprintf("topic.%s.channel.%s.backend_depth", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, channel.BackendDepth)

					stat = fmt.Sprintf("topic.%s.channel.%s.in_flight_count", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, int64(channel.InFlightCount))

					stat = fmt.Sprintf("topic.%s.channel.%s.deferred_count", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, int64(channel.DeferredCount))

					diff = channel.RequeueCount - lastChannel.RequeueCount
					stat = fmt.Sprintf("topic.%s.channel.%s.requeue_count", topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					diff = channel.TimeoutCount - lastChannel.TimeoutCount
					stat = fmt.Sprintf("topic.%s.channel.%s.timeout_count", topic.TopicName, channel.ChannelName)
					client.Incr(stat, int64(diff))

					stat = fmt.Sprintf("topic.%s.channel.%s.clients", topic.TopicName, channel.ChannelName)
					client.Gauge(stat, int64(channel.ClientCount))

					for _, item := range channel.E2eProcessingLatency.Percentiles {
						stat = fmt.Sprintf("topic.%s.channel.%s.e2e_processing_latency_%.0f", topic.TopicName, channel.ChannelName, item["quantile"]*100.0)
						client.Gauge(stat, int64(item["value"]))
					}
				}
			}
			lastStats = stats

			if n.getOpts().StatsdMemStats { // 内存统计信息
				ms := getMemStats()

				client.Gauge("mem.heap_objects", int64(ms.HeapObjects))
				client.Gauge("mem.heap_idle_bytes", int64(ms.HeapIdleBytes))
				client.Gauge("mem.heap_in_use_bytes", int64(ms.HeapInUseBytes))
				client.Gauge("mem.heap_released_bytes", int64(ms.HeapReleasedBytes))
				client.Gauge("mem.gc_pause_usec_100", int64(ms.GCPauseUsec100))
				client.Gauge("mem.gc_pause_usec_99", int64(ms.GCPauseUsec99))
				client.Gauge("mem.gc_pause_usec_95", int64(ms.GCPauseUsec95))
				client.Gauge("mem.next_gc_bytes", int64(ms.NextGCBytes))
				client.Incr("mem.gc_runs", int64(ms.GCTotalRuns-lastMemStats.GCTotalRuns))

				lastMemStats = ms
			}

			bw.Flush()
			sw.Flush()
			conn.Close()
		}
	}

exit:
	ticker.Stop()
	n.logf(LOG_INFO, "STATSD: closing")
}

func percentile(perc float64, arr []uint64, length int) uint64 {
	if length == 0 {
		return 0
	}
	indexOfPerc := int(math.Floor(((perc / 100.0) * float64(length)) + 0.5))
	if indexOfPerc >= length {
		indexOfPerc = length - 1
	}
	return arr[indexOfPerc]
}
