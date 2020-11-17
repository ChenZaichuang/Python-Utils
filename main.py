# 对于每个线程，要注意清理工作，清理工作还可以尽可能地释放内存
# gevent 要 catch GreenletExit, threading 要 catch SystemExit
# 每个IO操作，尤其是网络请求，要注意给timeout，避免kill不掉形成僵尸线程
