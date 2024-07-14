t = int(input())


while t>=0:
  t -= 1

  # Assume 'n' coins and assume 'q' queries:
  n,q = map(int,input().split())
  
  # Each coin(i) has value val[i] ; 0<=i<n;
  vals = list(map(int,input().split()))
  
  # Each query is of the form: 
    # flag X Y
      # flag : {0,1} ; If 0 then sum of values, if 1 then product of values ;
      # X : int ; 1 <= X <= n ;
      # Y : int ;
  flag, X, Y = map(int,input().split())
  
  # For each query, the position X contains a special coin.
  # Here, a coin is considered a special coin if:
    # The distance(D) of its index from the index of 
    # any other special coin is evenly divisible by Y.

    # D >= 0 and D%Y == 0 ; D = C - X ; for all coins indexed 'C' ;
    # Equivalently, C%Y == X%Y ; C >= X ; for all coins indexed 'C' ;
  
  # Find the aggregation of all special coins, in each query
  

  
  
  

  

  



