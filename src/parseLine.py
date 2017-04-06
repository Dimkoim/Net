def parseLine(line):
    '''Parse and split a string
    
    
    Args: 
        A string obejct
        
    Returns:
        List of strings    
    '''
    
    fields = line.split(',')
    clickdate = fields[1]
    url = fields[2]
    tracker_id = str(fields[3])
    key = str(fields[4])
    return (tracker_id, clickdate, url, key)
    
if __name__=='__main__':
    toy_data =  "134,2016-06-15 09:52:45,http://www.auping.com/nl/auping-plaza-roermond#appointment,1107c8de-5002-47ba-ad03-b78d7fed6b6c,i"
    tracker_id, clickdate, url, key =  parseLine(toy_data)