import name_node
import data_node
# import failure_detector
import test_fd
import threading
import socket
    

if __name__ == '__main__':
    HOST = socket.gethostname()
    MASTER_HOST = "fa22-cs425-2210.cs.illinois.edu"
    fd = test_fd.testFD()
    if HOST == MASTER_HOST:
        t0 = threading.Thread(target = name_node.run, args=[fd])
    # data node server thread
    t1 = threading.Thread(target = data_node.run_data_node)
    # failure detector
    t2 = threading.Thread(target = fd.run)
    
    if HOST == MASTER_HOST:
        t0.start()
    # print("t0 start")
    t1.start()
    # print("t1 start")
    t2.start()
    # print("t2 start")
    
    if HOST == MASTER_HOST:
        t0.join()
    t1.join()
    t2.join()

