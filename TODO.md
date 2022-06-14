
### To Do for v0.1.0

- [ ] Remove `anyhow` for errors and results
- [ ] Implement method/thread/worker to search for lost messages and re-queue (if configured)
- [ ] Implement method/thread/worker to purge & cleanup old/consumed messages
- [ ] Ensure all code is traced and instrumented
- [ ] Create unit-tests
  - [ ] Ensure `Message` has test cases around loading & unloading
  - [ ] Ensure `Message` has test cases for state changes and history
- [ ] Analysis situations where messages can be dropped without consumption
- [ ] Add toggle to disable active preloading of `pdn` map when a queue is created

### To Do for v0.2.0