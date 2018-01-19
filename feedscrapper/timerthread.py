from threading import Thread
from threading import Event
import time


class TimerThread(Thread):
    def __init__(self, event, delay, func, *args):
        Thread.__init__(self)
        self.stopped = event
        self.delay = delay
        self.func = func
        self.args = args

    def run(self):
        delay = 0
        while not self.stopped.wait(self.delay - delay):
            before = time.time()
            self.func(*self.args)
            delay = time.time() - before



# stopFlag = Event()
# thread = TimerThread(stopFlag, print_msg, "hello world", "msg2")
# thread.start()
#
# time.sleep(30)
# stopFlag.set()
