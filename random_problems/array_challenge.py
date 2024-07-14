n = int(input())
a = list(map(int,input().split()))

s = 0
res = []
for i in range(n):
  res.append(i*a[i] - s)
  s += a[i]

print(res)