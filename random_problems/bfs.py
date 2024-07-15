
n,m = map(int,input().split())
vertices = [[0 for j in range(m)] for i in range(n)]

dirs = [(0,1),(0,-1),(-1,0),(1,0)]

maxDegree = m*n

# flags: {True:'Identified', False:'Unexplored'}
_flags = [[False for j in range(m)] for i in range(n)]
flags = lambda  x,y: True if (x<0 or x>=n) or (y<0 or y>=m) else _flags[x][y]

id = 0
queue = [(0,0)]
while maxDegree>0 and size!=id:
  size = len(queue)
  while size != id:
    r,c = queue[id]
    id += 1

    if flags(r,c):
      continue

    # Perform computations ...

    _flags[r][c] = True
    for dr,dc in dirs:
      if not flags(r+dr,c+dc):
        queue.append((r+dr,c+dc))

  maxDegree -= 1


