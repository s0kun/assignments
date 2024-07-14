import random as R
import datetime as D
import os

rows = 10**7

# Strange Generator behaviour:
# a = (i for i in range(5))
# b = zip(a,a,a)
# print([v for v in b])

def nxt(count, g):
    try:
        while count > 0:
            yield g.__next__()
            count -= 1
    except StopIteration:
        pass



# Question 5

# id: int, start: date, end: date ;
header = 'id,start,end'

# start <= end ; id[i] != id[j] <-> i != j ;
glb = D.datetime(1800,1,1)
lub = D.datetime.now()

id = [i for i in range(rows)]

interval = (lub-glb).days
start = [glb + D.timedelta(days = R.randint(0,interval)) for i in range(rows)]
end = [glb + D.timedelta(days = R.randint(0,interval)) for i in range(rows)]

for i in range(rows):
    if (start[i]>end[i]):
        start[i],end[i] = end[i],start[i]

dat = (','.join(str(v) for v in r) for r in zip(id,start,end))

dname = 'Q5'
if not os.path.exists(dname):
    os.makedirs(dname)

ctr = 0
fname = 'dat'
while True:
    with open(os.path.join(dname,fname + str(ctr) + '.csv'),'w') as f:
        f.write(header + '\n')
        f.write('\n'.join(nxt(1<<ctr, dat)) + '\n')

    ctr += 1
    if (1 << ctr) - 1 >= rows:
        break




