import csv
import json
from dicttoxml import dicttoxml
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, MetaData, Table, Column, String, func


def save_data(query):
    def func_wrapper(*args, **kwargs):
        file_type = args[0].output_type
        result = []
        for row in query(*args, **kwargs):
            result.append(row)
        if file_type == 'table':
            args[0].insert(result, query.__name__)
            return
        with open(query.__name__ + "." + file_type, 'w', encoding='utf-8') as out:
            if file_type == 'csv':
                dict_writer = csv.DictWriter(out, result[0].keys())
                dict_writer.writeheader()
                dict_writer.writerows(result)
            elif file_type == 'json':
                json.dump(result, out)
            elif file_type == 'xml':
                out.write('''<?xml version="1.0" encoding="UTF-8" ?><root>''')
                [out.write(dicttoxml(i, root=False).decode()) for i in result]
                out.write("</root>")
        return query

    return func_wrapper


class SqlRunner:
    def __init__(self, path, file_type):
        self.output_type = file_type
        self.db = create_engine('sqlite:///{path}'.format(path=path))
        self.Session = sessionmaker(bind=self.db)
        self.meta = MetaData(self.db)
        self.meta.reflect(bind=self.db)
        self.tables = self.meta.tables

    def start_process(self):
        self.question1()
        self.question2()
        self.question3()
        self.question4()
        self.question5()
        self.question6()

    def insert(self, data, table_name):
        table = Table(table_name, self.meta)
        for column in data[0].keys():
            table.append_column(Column(column, String(50)))
        if table.exists():
            table.drop()
        table.create()
        con = self.db.connect()
        con.execute(table.insert(), data)

    @save_data
    def question1(self):
        artists = self.tables['artists']
        albums = self.tables['albums']
        tracks = self.tables['tracks']
        genres = self.tables['genres']
        s = self.Session()
        q = s.query(artists.c.Name, tracks.c.Name.label("song_name"), genres.c.Name.label("genre_name")). \
            filter(artists.c.ArtistId == albums.c.ArtistId). \
            filter(tracks.c.AlbumId == albums.c.AlbumId). \
            filter(genres.c.GenreId == tracks.c.GenreId)
        for i in q:
            yield {'song_name': i.song_name, 'artists': i.Name, 'genre': i.genre_name}

    @save_data
    def question2(self):
        customers = self.tables['customers']
        invoices = self.tables['invoices']
        s = self.Session()
        q = s.query(customers.c.FirstName, customers.c.LastName, customers.c.Email, customers.c.Address,
                    customers.c.City, customers.c.State, customers.c.Country,
                    func.count(invoices.c.InvoiceId).label('number_of_purchases')). \
            filter(customers.c.CustomerId == invoices.c.CustomerId). \
            group_by(customers.c.CustomerId)
        for i in q:
            yield {'first_name': i.FirstName, 'last_name': i.LastName, 'email': i.Email, 'address': i.Address,
                   'city': i.City, 'state': i.State, 'country': i.Country, 'number_of_purchases': i.number_of_purchases}

    @save_data
    def question3(self):
        result = self.db.execute(''' select country, substr(Email,
                                     instr(Email, '@') + 1) as Domain,
                                     count(substr(Email, instr(Email, '@') + 1) ) as Number
                                     from customers group by Domain''')
        for i in result:
            yield {'country': i.Country, 'domain': i.Domain, 'number': i.Number}

    @save_data
    def question4(self):
        result = self.db.execute('''select Country, sum(num)as num_of_sales from(
                                        select Country, count(invoice_items.InvoiceLineId) as num from customers
                                        join invoices on customers.CustomerId=invoices.CustomerId 
                                        join invoice_items on invoices.InvoiceId=invoice_items.InvoiceId
                                        join tracks on tracks.TrackId=invoice_items.TrackId
                                        join albums on albums.AlbumId=tracks.AlbumId
                                        group by albums.AlbumId)
                                    group by Country''')
        for i in result:
            yield {'country': i.Country, 'num_of_sales': i.num_of_sales}

    @save_data
    def question5(self):
        result = self.db.execute('''select Country, count(invoice_items.InvoiceLineId) as num from  customers
                                        join invoices on customers.CustomerId=invoices.CustomerId 
                                        join invoice_items on invoices.InvoiceId=invoice_items.InvoiceId
                                        group by Country''')
        for i in result:
            yield {'country': i.Country, 'num_of_sales': i.num}

    @save_data
    def question6(self):
        result = self.db.execute('''select Title, count(invoice_items.InvoiceLineId) as num from customers
                                        join invoices on customers.CustomerId=invoices.CustomerId 
                                        join invoice_items on invoices.InvoiceId=invoice_items.InvoiceId
                                        join tracks on tracks.TrackId=invoice_items.TrackId
                                        join albums on albums.AlbumId=tracks.AlbumId
                                        group by albums.AlbumId limit 1''')
        for i in result:
            yield {'album': i.Title, 'num_of_sales': i.num}

    @save_data
    def question7(self):
        result = self.db.execute('''select Title, count(invoice_items.InvoiceLineId) as num from customers
                                        join invoices on customers.CustomerId=invoices.CustomerId 
                                        join invoice_items on invoices.InvoiceId=invoice_items.InvoiceId
                                        join tracks on tracks.TrackId=invoice_items.TrackId
                                        join albums on albums.AlbumId=tracks.AlbumId
                                        where invoices.InvoiceDate >= 2011-01-01
                                        and Country = "USA"
                                        group by albums.AlbumId 
                                        order by num desc
                                        limit 1	''')
        for i in result:
            yield {'album': i.Title, 'num_of_sales': i.num}

    @save_data
    def question8(self):
        result = self.db.execute('''select count(Name) as number from tracks
                                    Where UPPER(Name) like "%BLACK%"''')
        for i in result:
            yield {'black_in_song_name': i.number}
