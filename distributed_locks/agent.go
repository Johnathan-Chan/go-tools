package distributed_locks

// the agent interface define

// Agent interface
type Agent interface {
	Lock(key string, timeout, renewTime int64) error
	UnLock(key string)
}