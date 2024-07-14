# The choice of opponent(Lily) for each round:
a = input()
# The number of rounds of Rock-Paper-Scissor played = 'n'
n = len(a)

MOD = 10**9 + 1

ops = ['R','P','S']
win_condition = {'R':'P', 'S':'R', 'P':'S'}

# Question: Calculate the number of possible ways to win the game of 'n' rounds
# assuming that WE DO NOT CHOOSE THE SAME 'OPS' IN CONSECUTIVE ROUNDS

# mem: Represents the count of distinct games, categorised by some identifying properties 'a','b','c'
# NOTE: 
  # The following categorisation of games is not disjoint owing to property 'c'
  # Also, interestingly, these properties a,b,c are independent of each other.

# mem[c] : Context of games where initial operation was NOT 'c' ('c' in {'R','P','S'})
# mem[c][a] : Context of games for sub-array corresponding to interval [a,n)
# mem[c][a][b] : Context of games with score-difference 'b'

# Q. Is ['c','a','b'] order of categorisation optimal?

# NOTE: Score-difference is calculated w.r.t. Lily (i.e. relative to Lily as 0-point)
mem = {
  'R': {i:{} for i in range(n)}, # This creates separate dicts for each 'i' right?
  'S': {i:{} for i in range(n)},
  'P': {i:{} for i in range(n)},
}

# Approach: The count of distinct games for sub-array [i+1, n)
# is related to the count of distinct games for sub-array [i,n)

# Q. Can this process be made more efficient? 
  # I think that it can, as changes are constrained to {-1,0,+1} in dimension-b

# Context of sub-array of rounds to consider:
for i in range(n-1,-1,-1):

  # Context of initial operation
  for j in ops:
    score = 0 if a[i]==j else (1 if win_condition[a[i]] == j else -1)

    # Context of score-difference 
    # Q. Can these computations be represented/denoted more efficiently? 
      # Seems to be constrained enough... ; assert (score in [-1,0,1])
    for diff, ctr in mem[j].get(i+1,{0:1}).items(): # {0:1} is base case
      if j != 'R':
        mem['R'][i][score+diff] = (mem['R'][i].get(score+diff,0) + ctr)
      if j != 'P':
        mem['P'][i][score+diff] = (mem['P'][i].get(score+diff,0) + ctr)
      if j != 'S':
        mem['S'][i][score+diff] = (mem['S'][i].get(score+diff,0) + ctr)

res = 0
for i in ops:
  for diff , ctr in mem[i][0].items():
    if diff > 0: # Since only considering games that are won
      res = (res + ctr)

# !R = P | S ; !P = S | R ; !S = P | R ; ['!' is NOT operator ; '|' is OR operator]
# Thus, (!R + !P + !S)/2 = (R + S + P) ; 

# NOTE: In-case of % abstraction, this does not necessarily hold 
assert (res%2 == 0) 
res = res//2 

# Answer:
print(res)

