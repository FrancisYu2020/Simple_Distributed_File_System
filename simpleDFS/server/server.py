import data_node
import name_node
import threading

t1 = threading.Thread(target = data_node.run_data_node)
t2 = threading.Thread(target = name_node.run_name_node)

t1.start()
t2.start()

t1.join()
t2.join()

