#####
# LIS 590: Socio-technical Data Analytics
# Project data preprocessing (SRFSS data 1993-1995, 2008-2011)
# Implemented by Qiyuan Liu
#####

# define delimiter
DELIMITER=','

# divide the raw data into several parts
def SeparateData(rawFilePath,numParts):
    """
    This function is used to separate the huge file into several parts.
    (if the client side computer is not powerfull enough, or we can run scripts on the server)
        * rawFilePath is the original data path
        * numParts is the number of parts/sub-files you wanna get
    """
    f_raw=open(rawFilePath,'rb')
    ls=f_raw.readlines()
    numLines=len(ls)
    interval=numLines/numParts
    f_raw.close()
    for num in range(numParts):
        f_new=open(rawFilePath+"_"+str(num)+".txt",'wb')
        for line in ls[num*interval:(num+1)*interval]:
            f_new.write(line)
        f_new.close()
        print "Generate new resized file_{0}",num
    print "{0} new resized files have been generated successfully!".format(numParts)

# fill all the lines of a file into a list
def GetData(filePath):
    f=open(filePath,'rb')
    ls=f.readlines()
    ls_data=[]
    for line in ls:
        ls_data.append(line.strip())
    print 'Gotten data at {0}'.format(filePath)
    return ls_data

# fill all the variable names into a list
# [startingColumn, variableName, fieldLength]
def GetVariables(filePath):
    f=open(filePath,'rb')
    ls_variableNames=f.readlines()
    ls_variableDesc=[]
    for line in ls_variableNames[1:]:
        if(len(line)>2):
            ls=line.split('\t')
            ls_variableDesc.append([ls[0],ls[1],ls[2].strip()])
    print "Gotten {0} variables.".format(len(ls_variableDesc)-1)
    return ls_variableDesc

# normalize the raw data (fill in delimeters '\t' between each column)
def NormalizeData(partFilePath, variableFilePath):
    """
    This function generates a normalized file with an extension of "_normalized.txt" in the same folder
        * partFilePath is one of the separated files.
        * variableFilePath is the file named "variableNameColumns.txt"
    """
    ls_data=GetData(partFilePath)
    # get variables
    ls_variableDesc=GetVariables(variableFilePath)
    ls_newData=[]
    for line in ls_data:
        ls_newEle=[]
        for v in ls_variableDesc:
            fieldLen=int(v[2])
            varName=v[1]
            startCol=int(v[0])
            ls_newEle.append(line[(startCol-1):(startCol-1+fieldLen)].strip())
        ls_newData.append(DELIMITER.join(ls_newEle))
    # add a variable name line into the normalized file
    line_variableName=[]
    for v in ls_variableDesc:
        line_variableName.append(v[1].strip())
    line_variableNames=DELIMITER.join(line_variableName)

    norFilePath=partFilePath+"_normalized.txt"
    f=open(norFilePath,'wb')
    f.write(line_variableNames+"\n")
    for line in ls_newData:
        f.write(line+"\n")
    f.close()
    print 'Generated normalized file ({1} lines) successfully at {0}:'.format(norFilePath,len(ls_newData))

# process data from 1993-1995 with the attributes in the same sequence as following:
# HowOftenEatFruit \t TotalNumServingFruit \t SummaryIndex \t
# TimeActivityPast \t TimeKeepActivityPast \t TimeActivity \t TimeKeepActivity
def ProcessDataYear(year):
    dirPath=r""
    rawFilePath=dirPath+r'data_'+year+'.ASC'
    SeparateData(rawFilePath,numParts=1) #no separation
    NormalizeData(dirPath+r'data_'+year+'.ASC_0.txt', dirPath+r'variableLayout_'+year+'.txt')

def MergeYearFile(year_list,savedFilePath):
    ls_data=[]
    ls_variable=[]
    for year in year_list:
        ls_variable=[]
        filePath=r''+'data_'+year+'.ASC_0.txt_normalized.txt'
        f=open(filePath,'rb')
        ls_variable=[x.strip() for x in f.readline().split('\t')]
        ls_variable.append('year')
        ls=f.readlines()
        for line in ls:
            ls_split=line.rstrip().split(DELIMITER)
            ls_split.append(year)
            ls_data.append(DELIMITER.join(ls_split))
        f.close()
    f=open(savedFilePath,'wb')
    f.write(DELIMITER.join(ls_variable)+'\n')
    for line in ls_data:
        f.write(line+'\n')
    f.close()
    print 'Merged file ({1} lines) to {0}'.format(savedFilePath,len(ls_data))  
        

if __name__ == "__main__":
    ProcessDataYear('1993')
    ProcessDataYear('1994')
    ProcessDataYear('1995')
    MergeYearFile(['1993','1994','1995'],r''+'mergedData1993-1995.txt')
    ProcessDataYear('2009')
    ProcessDataYear('2010')
    ProcessDataYear('2011')

    


