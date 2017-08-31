#coding=utf-8

"""
@varsion: ??
@author: 张帅男
@file: retest.py
@time: 2017/8/31 17:13
"""
import threading
import Queue
from test import rd

QUEUE1 = Queue.Queue(maxsize=1000)
for i in range(1000):
    i += 1
    QUEUE1.put(i)

QUEUE2 = Queue.Queue(maxsize=1000)
for i in range(1000):
    i += 1
    QUEUE2.put(i)

def set_key():
    key = QUEUE1.get()
    values = {
        "key": key
    }
    rd.shuainan.set(key, values)

def get_key():
    key = QUEUE2.get()
    print rd.shuainan.get(key)


if __name__ == '__main__':
    threads1 = []
    for i in range(100):
        t = threading.Thread(target=set_key())
        threads1.append(t)

    threads2 = []
    for i in range(50):
        t = threading.Thread(target=get_key())
        threads2.append(t)

    for t1 in threads1:
        t1.start()
        t1.join()

    for t2 in threads2:
        t2.start()
        t2.join()