import queue
class JobQueue:
    def __init__(self):
        self.q = queue.Queue()
    def push(self, job):
        self.q.put(job)
    def pop(self):
        return self.q.get()
