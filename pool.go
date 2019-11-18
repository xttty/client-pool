package pool

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
)

// Pool 连接池
type Pool struct {
	clients    chan *Client
	connCnt    int32
	cap        int32
	idleDur    time.Duration
	maxLifeDur time.Duration
	timeout    time.Duration //pool关闭时使用
	factor     Factor
	lock       sync.RWMutex
	mode       int
}

// Client 封装的gprc.ClientConn
type Client struct {
	*grpc.ClientConn
	timeUsed time.Time
	timeInit time.Time
	pool     *Pool
}

// Factor grpc连接工厂方法
type Factor func() (*grpc.ClientConn, error)

var (
	// ErrPoolInit 连接池初始化出错
	ErrPoolInit = errors.New("Pool init occurred error")
	// ErrGetTimeout 获取连接超时
	ErrGetTimeout = errors.New("Getting connection client timeout from pool")
	// ErrDialConn 创建连接发生错误
	ErrDialConn = errors.New("Dialing connection occurred error")
	// ErrPoolIsClosed 连接池已关闭
	ErrPoolIsClosed = errors.New("Pool is closed")
)

const (
	// PoolGetModeStrict 在实际创建连接数达上限后，池子中没有连接时不会新建连接
	PoolGetModeStrict = iota
	// PoolGetModeLoose 在实际创建连接数达上限后，池子中没有连接时会新建连接
	PoolGetModeLoose
)

// Default 初始化默认连接池
// idle: 10s
// max life time: 60s
// timeout: 10s
// mode: PoolGetModeLoose
func Default(factor Factor, init, cap int32) (*Pool, error) {
	return Init(factor, init, cap, 10*time.Second, 60*time.Second, 10*time.Second, PoolGetModeLoose)
}

// Init 初始化连接池
func Init(factor Factor, init, cap int32, idleDur, maxLifeDur, timeout time.Duration, mode int) (*Pool, error) {
	// 参数验证
	if factor == nil {
		return nil, ErrPoolInit
	}
	if init < 0 || cap <= 0 || idleDur < 0 || maxLifeDur < 0 {
		return nil, ErrPoolInit
	}
	// init pool
	if init > cap {
		init = cap
	}
	pool := &Pool{
		clients:    make(chan *Client, cap),
		cap:        cap,
		idleDur:    idleDur,
		maxLifeDur: maxLifeDur,
		timeout:    timeout,
		factor:     factor,
		mode:       mode,
	}
	// init client
	for i := int32(0); i < init; i++ {
		client, err := pool.createClient()
		if err != nil {
			return nil, ErrPoolInit
		}
		pool.clients <- client
	}
	return pool, nil
}

func (pool *Pool) createClient() (*Client, error) {
	conn, err := pool.factor()
	if err != nil {
		return nil, ErrPoolInit
	}
	now := time.Now()
	client := &Client{
		ClientConn: conn,
		timeUsed:   now,
		timeInit:   now,
		pool:       pool,
	}
	atomic.AddInt32(&pool.connCnt, 1)
	return client, nil
}

// Get 从连接池取出一个连接
func (pool *Pool) Get(ctx context.Context) (*Client, error) {
	if pool.IsClose() {
		return nil, ErrPoolIsClosed
	}

	var client *Client
	now := time.Now()
	select {
	case <-ctx.Done():
		if pool.mode == PoolGetModeStrict {
			pool.lock.Lock()
			defer pool.lock.Unlock()

			var client *Client
			var err error
			if pool.connCnt >= int32(pool.cap) {
				err = ErrGetTimeout
			} else {
				client, err = pool.createClient()
			}
			return client, err
		}
	case client = <-pool.clients:
		if client != nil && pool.idleDur > 0 && client.timeUsed.Add(pool.idleDur).After(now) {
			client.timeUsed = now
			return client, nil
		}
	}
	// 如果连接已经是idle连接，或者是非严格模式下没有获取连接
	// 则新建一个连接同时销毁原有idle连接
	if client != nil {
		client.Destory()
	}
	client, err := pool.createClient()
	if err != nil {
		return nil, err
	}
	return client, nil
}

// Close 连接池关闭
func (pool *Pool) Close() {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	if pool.IsClose() {
		return
	}

	clients := pool.clients
	pool.clients = nil

	// 异步处理池子里的连接
	go func() {
		for {
			select {
			case client := <-clients:
				if client != nil {
					client.Destory()
				}
			case <-time.Tick(pool.timeout):
				if len(clients) <= 0 {
					close(clients)
					break
				}
			}
		}
	}()
}

// IsClose 连接池是否关闭
func (pool *Pool) IsClose() bool {
	return pool == nil || pool.clients == nil
}

// Size 连接池中连接数
func (pool *Pool) Size() int {
	pool.lock.RLock()
	defer pool.lock.RUnlock()

	return len(pool.clients)
}

// ConnCnt 实际连接数
func (pool *Pool) ConnCnt() int32 {
	return pool.connCnt
}

// Close 连接关闭
func (client *Client) Close() {
	go func() {
		pool := client.pool
		now := time.Now()
		// 连接池关闭了直接销毁
		if pool.IsClose() {
			client.Destory()
			return
		}
		// 如果连接存活时间超长也直接销毁连接
		if pool.maxLifeDur > 0 && client.timeInit.Add(pool.maxLifeDur).Before(now) {
			client.Destory()
			return
		}
		if client.ClientConn == nil {
			return
		}
		client.timeUsed = now
		client.pool.clients <- client
	}()
}

// Destory 销毁client
func (client *Client) Destory() {
	if client.ClientConn != nil {
		client.ClientConn.Close()
		atomic.AddInt32(&client.pool.connCnt, -1)
	}
	client.ClientConn = nil
}

// TimeInit 获取连接创建时间
func (client *Client) TimeInit() time.Time {
	return client.timeInit
}

// TimeUsed 获取连接上一次使用时间
func (client *Client) TimeUsed() time.Time {
	return client.timeUsed
}
