#!/usr/bin/env python2.7

import psycopg2


class ReportRunner():
    '''ReportRunner class that creates various reports'''

    def __init__(self):
        self.dbName = 'news'

    def main(self):
        '''Main method that runs various log analysis reports.'''

        queries = []
        printInfo = []

        # Question 1
        printInfo.append(['Most popular three articles of all time',
                          ' views'])
        queries.append('''
            select a.title, total
            from articles as a
                left join (select path, count(*) as total
                           from log
                           where status = '200 OK'
                           group by path) as l
                on position(a.slug in l.path) > 0
            order by total desc
            limit 3
        ''')

        # Question 2
        printInfo.append(['Most popular article authors of all time',
                          ' views'])
        queries.append('''
            select au.name, sum(total)
            from articles as ar
                join authors as au on ar.author = au.id
                left join (select path, count(*) as total
                           from log
                           where status = '200 OK'
                           group by path) as l
                on position(ar.slug in l.path) > 0
            group by au.name
            order by count(au.name) desc
        ''')

        # Question 3
        printInfo.append(['Days with more than 1% of requests having errors',
                          '% errors'])
        queries.append('''
            select to_char(totals.Date, 'FMMonth FMDD, YYYY'),
                   to_char((errors.Total / totals.Total) * 100, 'FM999.0')
            from
            (select Date(time) as Date,
                    cast(count(1) as decimal) as Total
             from log
             where status like '4%' or status like '5%'
             group by Date) as errors
            inner join
            (select Date(time) as Date,
                    cast(count(1) as decimal) as Total
             from log
             group by Date) as totals
             on errors.Date = totals.Date
            where (errors.Total / totals.Total) > 0.01
        ''')

        # Loop through the queries, get results, print
        for i in range(len(queries)):
            results = self.run_query(queries[i])
            self.printer(printInfo[i][0], printInfo[i][1], results)

    def run_query(self, query):
        """Runs the given query and returns the results."""

        if not query:
            return None

        db, c = self.db_connect(self.dbName)
        c.execute(query)
        results = c.fetchall()
        db.close()
        return results

    def db_connect(self, db_name):
        """Connect to the PostgreSQL database.  Returns a database connection."""
        try:
            db = psycopg2.connect("dbname={}".format(db_name))
            c = db.cursor()
            return db, c
        except psycopg2.Error as e:
            print "Unable to connect to database"
            sys.exit(1) # The easier method

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
