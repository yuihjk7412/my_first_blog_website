import logging; logging.basicConfig(level = logging.INFO)
import asyncio, os, json, time, aiomysql, MySQLdb
from datetime import datetime 

from aiohttp import web

def index(request):
	return web.Response(boy = '''<h1>Welcome to Jack's blog</h1>''', content_type = 'text/html')

async def init(loop):
	app = web.Application(loop = loop)
	app.router.add_route('GET', '/', index)
	srv = await loop.create_server(app.make_handler(), '127.0.0.1', 9000)
	logging.info('server started at http://127.0.0.1:9000...')
	return srv

async def create_pool(loop, **kw):
	logging.info('create database connection pool...')
	global __pool
	__pool = await aiomysql.create_pool(
		host = kw.get('host', 'localhost'),
		port = kw.get('port', 3306), 
		user = kw['user'],
		password = kw['password']
		db = kw['db']
		charset = kw.get('autocommit', 'utf-8'),
		maxsize = kw.get('maxsize', 10),
		minsize = kw.get('minsize', 1)
		loop = loop 
		)

async def execute(sql, args):
	log(sql)
	with (await __pool) as conn:
		try:
			cur = await conn.cursor()
			await cur.execute(sql.replace('?', '%s'), args)
			affected = cur.rowcount
			await cur.close()
		except BaseException as e:
			raise
		return affected

async def select(sql, args, size=None):
	log(sql, args)
	global __pool
	with (await __pool) as conn:
		cur = await conn.cursor(aiomysql.DictCursor)
		await cur.execute(sql.replace('?', '%s'), args or ())
		if size:
			rs = await cur.fetchmany(size)
		else:
			rs = await cur.fetchall()
		await cur.close()
		logging.info('rows returned:%s'%len(rs))
		return rs

from orm import Model, StringField, IntegerField

class User(Model)
	__table__ = 'users'
	
	id = IntegerField(primary_key = True)
	name = StringField()
	
loop = asyncio.get_event_loop()
loop.run_until_complete(init(loop))
loop.run_forever()