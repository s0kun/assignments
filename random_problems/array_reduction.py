# Given 'n' integers A[0] ... A[n-1] all > 0 ;
# In one operation, 
  # Select any two indices 'i' and 'j',
  # such that A[i] > 0 and A[j] > 0.
  # Either replace A[i]

t = int(input())

while t>0:
  t -= 1

  n = int(input())
  a = list(map(int,input().split()))

  a.sort()
  a.append(0)

  ctr = 0
  s = 0
  for i in range(n):
    s += a[i]
    ctr += 1

    if 2*s < a[i+1]:
      ctr = 0

  print(ctr)
