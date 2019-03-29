import requests
from jira.client import JIRA
from json import JSONDecodeError


class Connection(object):
    '''Instantiate a JIRA client object to www.cocoa.volkswagen.de'''
    def __init__(self, username=None, password=None):
        self.url = 'https://cocoa.volkswagen.de/sjira/'
        self.username = username
        self.password = password
        self.jira = None
        self.authenticate()

    def authenticate(self):
        '''Authenticates user and gets session cookie.'''
        with requests.Session() as s:
            credentials = {}
            credentials['username'] = self.username
            credentials['password'] = self.password
            credentials['login-form-type'] = 'token'
            s.post('https://cocoa.volkswagen.de/pkmslogin.form', data=credentials)
            
        if s.cookies:
            self.status = 'Login successful!'
            self.cookies = s.cookies
            self.jira = self.client()
        else:
            self.status = 'Login failed! Please check username and/or password.'
            self.cookies = s.cookies
        
    def client(self):
        '''Creates JIRA client object or returns None if failed.'''
        try:
            jira_options={'server': self.url, 'cookies':self.cookies}
            jira=JIRA(options=jira_options, async_=True, async_workers=8)
            self.status = self.status + ' ' + f'You are logged in as {jira.current_user()}!'
            return jira
        except JSONDecodeError as j:
            self.status = 'Could not connect to JIRA instance. Please try again!'
            return