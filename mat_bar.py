#!/usr/bin/python

import matplotlib.pyplot as plt; plt.rcdefaults()
import numpy as np
import matplotlib.pyplot as plt

import sys
filename = sys.argv[1]

f = open(filename)
fd = f.readlines()

n = int(sys.argv[2])

fd1 = []
fd2 = []
for i in fd:
	a, b = i.split('\t')
	fd1.append(a)
	fd2.append(b)
#print fd2
i = 1
while (i < len(fd1) - 1):
	fd1[i] = ""
	i = i + 1
#print fd1


y_pos = np.arange(len(fd1))
#print y_pos
 
plt.bar(y_pos, fd2, align='center', alpha=1)
plt.xticks(y_pos, fd1)
str1 = str(1.0/n);
plt.title('Random numbers in intervals, step = ' + str1)
 
plt.show()
