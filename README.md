# Distributed Transaction System in Total Order

### Overview

In this project, we designed and implemented a distributed transaction system in total order using the [ISIS multicast algorithm](https://studylib.net/doc/7830646/isis-algorithm-for-total-ordering-of-messages). Total ordering means that, for every node in the distributed system, the orders of the transactions are totally the same. The system can also tolerate process failures until no node in the distributed system works.

The project is based on [ECE 428](https://ece.illinois.edu/academics/courses/ece428) @ UIUC, it is individual work.

System Start:

![start](https://github.com/TaKeTube/ECE-428/blob/main/MP1/start.gif?raw=true)

Failure Handling:

![failure_handling](https://github.com/TaKeTube/ECE-428/blob/main/MP1/failure_handling.gif?raw=true)

### Run Instruction

Copy <kbd>gentx.py</kbd>, <kbd>node.py</kbd>, <kbd>mp1_node</kbd> into folder <kbd>3_node_test</kbd> or <kbd>8_node_test</kbd>. Commands are listed in <kbd>localtest_scripts.txt</kbd>. Open a terminal for per command then run them all simultaneously. 

Interrupt by <kbd>crtl</kbd>+<kbd>C</kbd> to create node failure. 

### Protocol Design

We uses the [ISIS algorithm](https://studylib.net/doc/7830646/isis-algorithm-for-total-ordering-of-messages) with R-multicast. R-multicast ensures a reliable message delivery. ISIS algorithm ensures the total ordering.

### Failure Handling

When a failure happens, other nodes will notice this failure through exception thought by the socket. Then they will close the socket for the failed node, and change messages' deliverability according to the current node number in the queue. If the message is the feedback message and changes its status to deliverable, which means the node need to decide the agreed priority for this message, the node will mark this message as *need_to_be_multicast*. When this message is delivered, the node will decide its agreed priority then multicast the agreed priority to other nodes.

### Justification

Because it is ISIS algorithm, the correctness is justified.

### Results

#### Test 1

3 nodes, 0.5 Hz each, running for 100 seconds

<img src=".\3_node_test\result\Test1\node1.png" alt="node1" style="zoom:67%;" />

<img src=".\3_node_test\result\Test1\node2.png" alt="node2" style="zoom:67%;" />

<img src=".\3_node_test\result\Test1\node3.png" alt="node3" style="zoom:67%;" />

#### Test 2

8 nodes, 5 Hz each, running for 100 seconds

<img src=".\8_node_test\result\Test2\node1.png" alt="node1" style="zoom:67%;" />

<img src=".\8_node_test\result\Test2\node2.png" alt="node2" style="zoom:67%;" />

<img src=".\8_node_test\result\Test2\node3.png" alt="node3" style="zoom:67%;" />

<img src=".\8_node_test\result\Test2\node4.png" alt="node4" style="zoom:67%;" />

<img src=".\8_node_test\result\Test2\node5.png" alt="node5" style="zoom:67%;" />

<img src=".\8_node_test\result\Test2\node6.png" alt="node6" style="zoom:67%;" />

<img src=".\8_node_test\result\Test2\node7.png" alt="node7" style="zoom:67%;" />

<img src=".\8_node_test\result\Test2\node8.png" alt="node8" style="zoom:67%;" />

#### Test 3

3 nodes, 0.5 Hz each, running for 100 seconds, then one node fails, and the rest continue to run for 100 seconds

<img src=".\3_node_test\result\Test3\node1.png" alt="node1" style="zoom:67%;" />

<img src=".\3_node_test\result\Test3\node2.png" alt="node2" style="zoom:67%;" />

<img src=".\3_node_test\result\Test3\node3.png" alt="node3" style="zoom:67%;" />

#### Test 4

8 nodes, 5 Hz each, running for 100 seconds, then 3 nodes fail simultaneously, and the  rest continue to run for 100 seconds.

<img src=".\8_node_test\result\Test4\node1.png" alt="node1" style="zoom:67%;" />

<img src=".\8_node_test\result\Test4\node2.png" alt="node2" style="zoom:67%;" />

<img src=".\8_node_test\result\Test4\node3.png" alt="node3" style="zoom:67%;" />

<img src=".\8_node_test\result\Test4\node4.png" alt="node4" style="zoom:67%;" />

<img src=".\8_node_test\result\Test4\node5.png" alt="node5" style="zoom:67%;" />

<img src=".\8_node_test\result\Test4\node6.png" alt="node6" style="zoom:67%;" />

<img src=".\8_node_test\result\Test4\node7.png" alt="node7" style="zoom:67%;" />

<img src=".\8_node_test\result\Test4\node8.png" alt="node8" style="zoom:67%;" />
