class Solution(object):

    def flip(s,r,c):
        """
        s: 'Solution' object ; 'self' reference
        r: int ; row index
        c: int ; column index
        returns None
        
        flip(r,c) decides if the position (r,c) must be flipped to 
        a 'O'/True under the given conditions:
            If (r,c) is on the border of s.m*s.n matrix and 
            was an 'O' in the 's.board' matrix, OR
            If (r,c) is edge-adjacent to a *flipped* 'O'  

        Also, flip(r,c) must update positions that are 
        consequently affected by the flipping of position (r,c) ;

        Consider all positions to be initially be flipped to 'X'/False ;
        """
        if((not (0<=r and r<s.n and 0<=c and c<s.m)) or s.flp[r][c]):
            return
        
        if(r==s.n-1 or c==s.m-1 or r==0 or c==0):
            s.flp[r][c] = (s.board[r][c]=='O')
        else:
            s.flp[r][c] = (
                s.board[r][c]=='O' and 
                (s.flp[r+1][c] or s.flp[r][c-1] or s.flp[r-1][c] or s.flp[r][c+1])
            )

        # Only if the position if flipped 
        # to 'O', we invoke the adjacent positions.
        if(s.flp[r][c]):
            s.flip(r+1,c)
            s.flip(r,c+1)
            s.flip(r-1,c)
            s.flip(r,c-1)
        
        return

    def solve(self, board):
        """
        :type board: List[List[str]]
        :rtype: None Do not return anything, modify board in-place instead.
        """
        self.board = board
        self.n = len(board)
        self.m = len(board[0])
        self.flp = [[False for i in range(self.m)] for j in range(self.n)]

        # Initiate flipping of border positions.
        # All other flips are consequential.
        for i in range(self.m):
            self.flip(0,i)
            self.flip(self.n-1,i)
        for i in range(self.n):
            self.flip(i,0)
            self.flip(i,self.m-1)

        for i in range(self.n):
            for j in range(self.m):
                self.board[i][j] = 'O' if (self.flp[i][j]) else 'X'

        return self.board

def main():
    n,m = map(int,input().split())
    board = [list(input().split()) for i in range(n)]

    obj = Solution()
    obj.solve(board)

    for i in range(n):
        for j in range(m):
            print(board[i][j],end=' ')
        print()


if(__name__ == '__main__'):
    main()
