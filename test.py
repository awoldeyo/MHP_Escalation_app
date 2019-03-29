from concurrent.futures import ProcessPoolExecutor
from concurrent import futures
from time import sleep


class Question(object):
    
    def __init__(self):
        self.questions = ['What?', 'When', 'Where']
        self.answers = ['This', 'Then', 'There']
        self.results = []
    
    
    def upper(self):
        return [i.upper() for i in self.answers], [i.upper() for i in self.questions]
        
    def merge(self):
        return list(zip(self.questions,self.answers))
    
    def start_process(self):
        self.results = self.upper()
        self.merge()


q = Question()
with ProcessPoolExecutor() as executor:
    ex1 = executor.submit(q.upper)
    ex2 = executor.submit(q.merge)