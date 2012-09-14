
def levenshtein(a,b):
    "Calculates the Levenshtein distance between a and b."
    n, m = len(a), len(b)
    if n > m:
        # Make sure n <= m, to use O(min(n,m)) space
        a,b = b,a
        n,m = m,n
        
    current = range(n+1)
    for i in range(1,m+1):
        previous, current = current, [i]+[0]*n
        for j in range(1,n+1):
            add, delete = previous[j]+1, current[j-1]+1
            change = previous[j-1]
            if a[j-1] != b[i-1]:
                change = change + 1
            current[j] = min(add, delete, change)
            
    return current[n]
    
def levenshtein_lowest(string, options):
    "Returns items from options with lowest levenshtein distance to string"
    lowest = {
      'dist': 100,
      'item': None
    }
    for o in options:
      l = levenshtein(string, o)
      if l < lowest['dist']:
        lowest['dist'] = l
        lowest['item'] = o
    
    return lowest['item']
    