n = int(input())
start = list(map(int,input().split()))
stop = list(map(int,input().split()))

ord = [i for i in range(n)]
ord.sort(key = lambda x: start[x])

start.sort()

stop = [stop[i] for i in ord]

# print(start)
# print(stop)

mx = ctr = 0
i = j = 0
while i < n:
  ctr += 1
  
  while start[i] > stop[j]:
    j += 1
    ctr -= 1

  mx = max(mx, ctr)
  i += 1

print(mx)