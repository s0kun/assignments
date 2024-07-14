# Given an array of integers P, of size N.

# Assumed partition(X) notation:
    # X : [X0, X1, X2, ..., X(m-1)], where
        # 0 <= X0 < X1 < ... < X(m-1) < N ;
        # X[-1] = 0 ; X[m] = N ;
    # Sub-arrays are: [X[k-1], X(k) - 1],  for all 0 <= k <= m ;

# For any given partition(X) of the array P,
# its score is defined as:
    # Sum over k from '0' to 'm': (
        # (Max over q from X[k-1] to X[k]-1: P[q]) -
        # (Min over q from X[k-1] to X[k]-1: P[q])
    # )

# Additionally, the size of a partition sub-array is:
    # Either 1 or belonging to [A,B], where
    # 0 <= B - A <= 500 ;

# Find a partition X that maximized the score for P
# under given constraints. Output its corresponding score.

import math

n = int(input())
a = int(input())
b = int(input())

p = list(map(int,input().split()))

class Data:
    def __init__(self,mx,mn):
        self.mx =  mx
        self.mn = mn

    def __repr__(self):
        return f"Data(mx={self.mx} mn={self.mn})"
        # return f"({self.mx},{self.mn})"

size = 1<<math.ceil(math.log2(n+1))
_op = [None for i in range(2*size - 1)]

# Calculate '_op'
for i in range(size):
    _op[i + size - 1] = Data(p[i],p[i]) if i < n else Data(-float("inf"),float("inf"))
for i in range(size-1, 0, -1):
    T = i<<1
    _op[i-1] = Data(
        max(_op[T-1].mx, _op[T].mx),
        min(_op[T-1].mn, _op[T].mn)
    )

def op(i,j, pos=1, l = 1, r = size) -> Data:

    # # Alternative, Time: O(1) ; Space: O(2*log2(size)) ; (Explicitly pass (l,r) arguments instead of decoding from 'pos')

    # # Transform index 'pos' to range-pair (l,r) ; Time: O(log2(pos)) ; Space: O(1) ;
    # r = size
    # l = 1
    # X = ( 1<<int(math.log2(pos)) ) >> 1
    # while X != 0:
    #     if (pos&X)==0:
    #         r -= (r-l+1)>>1
    #     else:
    #         l += (r-l+1)>>1
    #     X = X >> 1

    # print(f"pos:{pos}, l:{l}, r:{r}, i:{i}, j:{j}")

    if r==(j+1) and l==(i+1):
        return _op[pos-1]

    m = (l + r)//2
    flag = (j >= m)*1 + (i <= m-1)*2

    if flag == 3:
        # T1 = op(m, j, (pos<<1)+1)
        # T2 = op(i, m-1, pos<<1)

        T1 = op(m, j, (pos<<1)+1, m+1, r)
        T2 = op(i, m-1, pos<<1, l, m)

        # Operation:
        return Data(
            max(T1.mx, T2.mx),
            min(T1.mn, T2.mn)
        )
    elif flag == 2:
        # return op(i,j,pos<<1)
        return op(i,j,pos<<1, l, m)
    elif flag == 1:
        # return op(i,j,(pos<<1) + 1)
        return op(i,j,(pos<<1) + 1, m+1, r)

    raise RuntimeException("Improper arguments.")


res = [0 for i in range(n+1)]
for i in range(n-1,-1,-1):
    res[i] = res[i+1]
    for j in range(i+a-1,min(i+b,n)):
        res[i] = max(res[i],res[j+1] + op(i,j).mx - op(i,j).mn)

print(res[0])
