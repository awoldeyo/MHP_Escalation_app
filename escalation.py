import os
from json import JSONDecodeError
import re

import pandas as pd
from pandas.errors import OutOfBoundsDatetime
from jira.exceptions import JIRAError
from jira_cache import CachedIssues

from format_df import JiraDf


def to_datetime(string):
    '''Checks if date is valid. Returns None in case of invalid date.'''
    try:
        return pd.to_datetime(string, dayfirst=True)
    except OutOfBoundsDatetime as o:
        return None

def get_changelog(project_issues):
    '''
       Returns information on when due date/due date implemented
       was changed. For changes before 30.10.2018 only the 
       due date field is considered, for every change after 
       30.10.2018 only due date implemented field is considered.
    '''
    duedate = []
    for issue in project_issues:
        changelog = issue.changelog
        for history in changelog.histories:
            for item in history.items:
                created_dt = pd.to_datetime(history.created, dayfirst=True)
                is_before_change = created_dt <= pd.to_datetime('30.10.2018')
                if is_before_change and item.field == "duedate":
                    row = {}
                    row['id'] = issue.id
                    row['date'] = to_datetime(history.created)
                    row['from'] = to_datetime(item.fromString)
                    row['to'] = to_datetime(item.toString)
                    duedate.append(row)
                if item.field == "Due Date Implemented":
                    row = {}
                    row['id'] = issue.id
                    row['date'] = to_datetime(history.created)
                    row['from'] = to_datetime(item.fromString)
                    row['to'] = to_datetime(item.toString)
                    duedate.append(row)
    return pd.DataFrame(duedate)


def create_datetable(df):
    '''
       Returns a reshaped changelog dataframe, so that every row
       becomes a separate column ('Due Date 1', 'Due Date 2', etc.).
    '''
    df = df.sort_values(['id', 'date'], ascending=False)
    df = df.set_index('id').sort_values(['id', 'date'], ascending=True)
    
    # Calculate the maximum required number of columns
    cols = df.groupby('id').count()['date'].max()
    # Define name of columns ("Due Date 1", "Due Date 2", etc.)
    col_names = [f'Due Date {i+1}' for i in range(cols)]

    table = []
    for idx in df.index.unique():
        row = {}
        row['id'] = idx
        for n,date in enumerate(df.loc[[idx], 'to'].sort_values(ascending=True).drop_duplicates()):
            row[col_names[n]] = date
        table.append(row)
    return pd.DataFrame(table).set_index('id')

class EscalationReport(object):
    def __init__(self, jira_client, filename):
        self.jira = jira_client
        self.filename = filename
        self.issues_in_project = None
        self.status = None
        try:
            self.status = self.get_updated_issues()

        except (FileNotFoundError, IndexError):
            self.status =  self.get_all_issues()
            
        finally:
            if self.issues_in_project:
                self.status = self.generate_report()
                
    def get_updated_issues(self):
        for file in os.listdir('./cachedIssues/'):
            if file.endswith('.json'):
                cache_filepath = os.path.join('./cachedIssues/', file)
                self.cached_issues = CachedIssues.load(open(cache_filepath))
                break
        
        parsed_date = re.findall('\d+-\d+-\d+', file)[0]
        self.parsed_date = pd.to_datetime(parsed_date).strftime('%Y-%m-%d')
        search_string = f'''project = DC AND
                            labels in (VW-PKW, VW-PKW_InKlaerungKILX) AND
                            updated >= {self.parsed_date} AND
                            updated <= now()'''
        self.issues_in_project = self.jira.search_issues(
                jql_str=search_string,
                maxResults=False,
                expand='changelog')
        
        # Define keys that need to be dropped from cache
        drop_keys = [i.key for i in self.issues_in_project]
        
        for issue in self.cached_issues:
            if issue.key not in drop_keys:
                self.issues_in_project.append(issue)
                
        self.cache_results()
        
        return f'Collected {len(self.issues_in_project)} issues. Generating report...'
    
    def get_all_issues(self):
        try:
            self.issues_in_project = self.jira.search_issues(
                    jql_str='project = DC AND labels in (VW-PKW, VW-PKW_InKlaerungKILX)',
                    maxResults=False,
                    expand='changelog')
            
            self.cache_results()
            
            return f'Collected {len(self.issues_in_project)} issues. Generating report...'
        
        except JSONDecodeError as j:
            self.issues_in_project = None
            return 'Session expired. Please login again.'
        
        except JIRAError as j:
            self.issues_in_project = None
            return j.text
    
    def cache_results(self):
        cached_results = CachedIssues(self.issues_in_project)
        cache_path = './cachedIssues/'
        if not os.path.exists(cache_path):
            os.mkdir(cache_path)
        today = pd.datetime.now().strftime('%d-%m-%Y')
        cached_results.dump(open(f'{cache_path}DC_Issues_{today}.json', 'w'))
        
        for file in os.listdir(cache_path):
            if file.endswith('.json') and file != f'DC_Issues_{today}.json':
                os.remove(os.path.join(cache_path, file))
        
    
    def generate_report(self):
        # Generate duedate changelog dataframe
        duedate = get_changelog(self.issues_in_project)
        duedate_reshaped = create_datetable(duedate)
        
        # Generate issues dataframe
        issues_df = JiraDf(issues=self.issues_in_project,
                           jira_client=self.jira, 
                           frontendcolname=True, 
                           stringvalues=True).df
        
        # Prepare issues dataframe columns
        issues_df.columns = [c.title() if c =='status' else c for c in issues_df.columns]
        cols = [
                'id',
                'key',
                'Department',
                'Component/s',
                'Detailed Type',
                'Reporter',
                'Assignee',
                'Contact Person (Business department)',
                'Contact Person (IT)',
                'Business Transaction',
                'Affected IT-System',
                'Summary',
                'Status',
                'Handover Date',
                'Dokumente vorhanden?',
                ]
        issues_df.dropna(axis=1, how='all', inplace=True)
        issues_df = issues_df.reindex(cols, axis=1)
        new_colname = [
                'id',
                'JIRA ID', 
                'Bereich', 
                'Component/s', 
                'Detailed Type', 
                'Assignee', 
                'Reporter',
                'Contact Person (Business department)', 
                'Contact Person (IT)',
                'Business Transaction', 
                'System', 
                'Maßnahme', 
                'Status',
                'Maßnahme übergeben am:', 
                'Dokumente vorhanden?',
                ]
        issues_df.columns = new_colname
        
        # Fill n/a values with empty string
        issues_df.fillna('', inplace=True)
        
        # Create Hyperlinks in column JIRA DF
        url = 'https://cocoa.volkswagen.de/sjira/browse/'
        create_url = lambda x: f'=HYPERLINK("{url}{x}", "{x}")'
        issues_df['JIRA ID'] = issues_df['JIRA ID'].map(create_url)
        
        # Merge duedate changelog and issues dataframe
        final_df = pd.merge(issues_df, duedate_reshaped, how='outer', on='id')
        
        # Clean up final dataframe
        final_df = final_df.drop('id', axis=1).fillna('')
        date_time_cols = [c for c in final_df.columns if 'date' in c.lower()]
        date_time_cols += ['Maßnahme übergeben am:']
        final_df[date_time_cols] = final_df[date_time_cols].apply(lambda x: pd.to_datetime(x, dayfirst=True))
        final_df.to_excel(f'{self.filename}', index=False, sheet_name='Maßnahmen')
        
        return f'Successfully generated report!\n Path:{self.filename}'