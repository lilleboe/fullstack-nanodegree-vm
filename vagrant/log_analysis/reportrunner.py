#!/usr/bin/env python2.7

import psycopg2


class ReportRunner():
    '''ReportRunner class that creates various reports'''

    def __init__(self):
        self.dbName = 'news'

    def main(self):
        '''Main method that runs various log analysis reports.'''

        queries = []
        print_info = []

        # Question 1
        print_info.append(['What are the most popular three articles of all time?', ' views'])
        queries.append('''
            select a.title, count(a.title)
            from articles as a
                left join log as l on position(a.slug in l.path) > 0
            group by a.title
            order by count(a.title) desc
            limit 3
        ''')
        
        # Question 2
        print_info.append(['Who are the most popular article authors of all time?', ' views'])
        queries.append('''
            select au.name, count(au.name)
            from articles as ar
                join authors as au on ar.author = au.id
                left join log as l on position(ar.slug in l.path) > 0
            group by au.name
            order by count(au.name) desc
        ''')
        
        # Question 3
        print_info.append(['On which days did more than 1% of requests lead to errors?', '% errors'])
        queries.append('''
            select totals.Date, to_char((errors.Total / totals.Total) * 100, 'FM999.0')
            from
            (select to_char(time, 'Mon DD, YYYY') as Date, cast(count(1) as decimal) as Total
             from log
             where status like '4%' or status like '5%'
             group by Date) as errors
            inner join
            (select to_char(time, 'Mon DD, YYYY') as Date, cast(count(1) as decimal) as Total
             from log
             group by Date) as totals
             on errors.Date = totals.Date
            where (errors.Total / totals.Total) > 0.01
        ''')

        # Loop through the queries, get results, print
        for i in range(len(queries)):
            results = self.run_query(queries[i])
            self.printer(print_info[i][0], print_info[i][1], results)

    def run_query(self, query):
        """Runs the given query and returns the results."""

        if not query:
            return None

        db = psycopg2.connect(database=self.dbName)
        c = db.cursor()
        c.execute(query)
        results = c.fetchall()
        db.close()
        return results

    def printer(self, title, end, data):
        '''Produces a readable output'''

        print ""
        print "=" * 60
        print title
        print "-" * 60
        for row in data:
            print " -- ".join([str(x) for x in row]) + end
        print "=" * 60
        print ""

if __name__ == '__main__':

    rr = ReportRunner()
    rr.main()
