import os
'''
Class : sourceControl
    Description :
    A class processing file IO on the source received.
'''
class sourceControl:
    preCode = None
    body = None
    postCode = None
    '''
    Function : __init__(...)
        Description :
        A function that initializes the class
        Parameters :
            preCode : A source executed prior to the body source
            body : A source executed in the middle. User input from the dashboard is required.
            postCode : A source that wraps up the whole process.

        BE ADVISED :
        Unless there is a reason to modify both the preCode and postCode,
        do not put anything on both the preCode and postCode parameter.
    '''
    def __init__(self, preCode = None, body = None, postCode = None, _mapId_hashed_string = None):
        self.preCode = preCode
        self.body = body
        self.postCode = postCode
        self.mapId_hashed_string = _mapId_hashed_string

        self.storeSources()
    '''
    Function : storeSources()
        Descriptions :
        A function that stores the sources received.
    '''
    def storeSources(self):
        '''
        if not self.preCode or not self.body or self.postCode:
            print("At least one of the source is empty\n")
        '''
        fileDir = os.path.dirname(os.path.realpath('__file__'))
        pathbodyCode = os.path.join(fileDir, 'enow/jython/pythonSrc', self.mapId_hashed_string, 'body.py')
        
        with open(pathbodyCode, "wb") as file:
            file.seek(0)
            file.truncate()
            file.write(self.body)
