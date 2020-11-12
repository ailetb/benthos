package buffer

import (
	"sync/atomic"
	"time"

	"github.com/Jeffail/benthos/v3/lib/log"
	"github.com/Jeffail/benthos/v3/lib/message/batch"
	"github.com/Jeffail/benthos/v3/lib/metrics"
	"github.com/Jeffail/benthos/v3/lib/types"
)

//------------------------------------------------------------------------------

// ParallelBatcher wraps a buffer with a Producer/Consumer interface.
type ParallelBatcher struct {
	stats metrics.Type
	log   log.Modular
	conf  Config

	child   Type
	batcher *batch.Policy

	messagesOut chan types.Transaction

	running    int32
	closeChan  chan struct{}
	closedChan chan struct{}
}

// NewParallelBatcher creates a new Producer/Consumer around a buffer.
func NewParallelBatcher(
	batcher *batch.Policy,
	child Type,
	log log.Modular,
	stats metrics.Type,
) Type {
	m := ParallelBatcher{
		stats:       stats,
		log:         log,
		child:       child,
		batcher:     batcher,
		messagesOut: make(chan types.Transaction),
		running:     1,
		closeChan:   make(chan struct{}),
		closedChan:  make(chan struct{}),
	}
	return &m
}

//------------------------------------------------------------------------------

// outputLoop is an internal loop brokers buffer messages to output pipe.
func (m *ParallelBatcher) outputLoop() {
	defer func() {
		m.child.CloseAsync()
		err := m.child.WaitForClose(time.Second)
		for err != nil {
			err = m.child.WaitForClose(time.Second)
		}
		m.batcher.CloseAsync()
		err = m.batcher.WaitForClose(time.Second)
		for err != nil {
			err = m.batcher.WaitForClose(time.Second)
		}
		close(m.messagesOut)
		close(m.closedChan)
	}()

	var nextTimedBatchChan <-chan time.Time
	if tNext := m.batcher.UntilNext(); tNext >= 0 {
		nextTimedBatchChan = time.After(tNext)
	}

	var pendingResChans []chan<- types.Response
	for atomic.LoadInt32(&m.running) == 1 {
		if nextTimedBatchChan == nil {
			if tNext := m.batcher.UntilNext(); tNext >= 0 {
				nextTimedBatchChan = time.After(tNext)
			}
		}

		var flushBatch bool
		select {

		//step 3.2.6 订阅ParallelWrapper产生的每一条消息
		case tran, open := <-m.child.TransactionChan():
			if !open {
				// Final flush of remaining documents.
				atomic.StoreInt32(&m.running, 0)
				flushBatch = true
				// If we're waiting for a timed batch then we will respect it.
				if nextTimedBatchChan != nil {
					select {
					case <-nextTimedBatchChan:
					case <-m.closeChan:
						return
					}
				}
			} else {
				tran.Payload.Iter(func(i int, p types.Part) error {
					//step 3.2.7 将消息放到批量缓存区，如添加消息后满足flush的条件，则设置flushBatch = true
					if m.batcher.Add(p) {
						flushBatch = true
					}
					return nil
				})
				//step 3.2.8 消息未达到批量刷新条件，则放到等待队列
				pendingResChans = append(pendingResChans, tran.ResponseChan)
			}
		case <-nextTimedBatchChan:
			//step 3.2.9 达到刷新时间间隔，触发flush
			flushBatch = true
			nextTimedBatchChan = nil
		case <-m.closeChan:
			return
		}

		if !flushBatch {
			continue
		}

		//step 3.2.10 开始flush，批量输出消息
		sendMsg := m.batcher.Flush()
		if sendMsg == nil {
			continue
		}

		resChan := make(chan types.Response)
		select {
		//step 3.2.11 将消息放到buffer输出管道messagesOut，messagesOut产生的消息会被processor消费（-->>step 3.3.2）
		case m.messagesOut <- types.NewTransaction(sendMsg, resChan):
		case <-m.closeChan:
			return
		}

		//step 3.2.12异步监听消息处理结果，给每一条消息设置一个response。 （用于给input（kafka、file）环节实现ACK功能）
		go func(rChan chan types.Response, upstreamResChans []chan<- types.Response) {
			select {
			case <-m.closeChan:
				return
			case res, open := <-rChan:
				if !open {
					return
				}
				for _, c := range upstreamResChans {
					select {
					case <-m.closeChan:
						return
					case c <- res:
					}
				}
			}
		}(resChan, pendingResChans)
		pendingResChans = nil
	}
}

// Consume assigns a messages channel for the output to read.
func (m *ParallelBatcher) Consume(msgs <-chan types.Transaction) error {

	//step 3.2.1 先由ParallelWrapper 异步缓冲输入、输出每一条消息
	if err := m.child.Consume(msgs); err != nil {
		return err
	}

	//step 3.2.5 通过NewParallelBatcher批量输出消息
	go m.outputLoop()
	return nil
}

// TransactionChan returns the channel used for consuming messages from this
// buffer.
func (m *ParallelBatcher) TransactionChan() <-chan types.Transaction {
	return m.messagesOut
}

// CloseAsync shuts down the ParallelBatcher and stops processing messages.
func (m *ParallelBatcher) CloseAsync() {
	m.child.CloseAsync()
	if atomic.CompareAndSwapInt32(&m.running, 1, 0) {
		close(m.closeChan)
	}
}

// StopConsuming instructs the buffer to stop consuming messages and close once
// the buffer is empty.
func (m *ParallelBatcher) StopConsuming() {
	m.child.StopConsuming()
}

// WaitForClose blocks until the ParallelBatcher output has closed down.
func (m *ParallelBatcher) WaitForClose(timeout time.Duration) error {
	select {
	case <-m.closedChan:
	case <-time.After(timeout):
		return types.ErrTimeout
	}
	return nil
}

//------------------------------------------------------------------------------
