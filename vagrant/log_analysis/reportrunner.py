
#!/usr/bin/env python2.7

import psycopg2

class ReportRunner():
    '''ReportRunner class that creates various reports'''
    

    def __init__(self):
        self.dbName = 'news'

    def main(self):
      '''Main method that runs various log analysis reports.'''

      query1 = '''
        select a.title, count(a.title)
        from articles as a 
            left join log as l on position(a.slug in l.path) > 0
        group by a.title
        order by count(a.title) desc
      '''      

      results = self.run_query(query1)
      self.pretty_print(results)
      
      query2 = '''
        select au.name, count(au.name)
        from articles as ar 
            join authors as au on ar.author = au.id
            left join log as l on position(ar.slug in l.path) > 0
        group by au.name
        order by count(au.name) desc
      '''      

      results = self.run_query(query2)
      self.pretty_print(results)

      query3 = '''
        select au.name, count(au.name)
        from articles as ar 
            join authors as au on ar.author = au.id
            left join log as l on position(ar.slug in l.path) > 0
        group by au.name
        order by count(au.name) desc
      '''      

      results = self.run_query(query3)
      self.pretty_print(results)



      return 0

    def run_query(self, query):
      """Runs the given query and returns the results."""

      if not query:
          return None

      db = psycopg2.connect(database=self.dbName)
      c = db.cursor()
      #print(query)
      c.execute(query)
      results = c.fetchall()
      db.close()
      return results

    def pretty_print(self, data):
        '''Takes data input and produces a readable output'''
        print("----------------------------------")
        for row in data:
            print(row)
 

if __name__ == '__main__':
    rr = ReportRunner()
    rr.main()