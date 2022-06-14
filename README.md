## etcdmq

A simple message queue on top of etcd


two inputs- watcher, and lister.
watcher subscribes to events in the key space, and adds key ID into the queue
lister lists all keys in the key space, and adds each item to the queue

- the queue is read in a loop. a job id is selected
- client starts a transaction
- checks if key exists
- if it does, delete key
- if it does not, list keys, and try a random key
- read job data from /job
- return Task<T> object
    - on drop, Task moves the task state from TaskState::CONSUMED to TaskState::COMPLETED