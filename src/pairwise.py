from itertools import izip, tee

def pairwise(iterable):
    '''A pairwise iterator
    (p0, p1, ..., pn) -> (p0, p1), (p1, p2), ..., (pn-1, pn)
    
    Args: 
        List of strings
    
    Returns: 
        A list of 
    '''
    
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)


if __name__=='__main__':
    toy_data =  ('a', 'b', 'c', 'd')
    result = list(pairwise(toy_data))
    