import logging; logging.basicConfig(level = logging.INFO)
import asyncio, os, json, time, aiomysql, MySQLdb
from datetime import datetime
from orm import Model, StringField, IntegerField 

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
		password = kw['password'],
		db = kw['db'],
		charset = kw.get('autocommit', 'utf-8'),
		maxsize = kw.get('maxsize', 10),
		minsize = kw.get('minsize', 1),
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

class  User(Model):
	__table__ = 'users'
	
	id = IntegerField(primary_key = True)
	name = StringField()


class Model(dict, metaclass = ModelMetaclass):
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	'''@classmethod
	@asyncio.coroutine
	def find(cls, ProcessLookupError):
		'find object by primary key'
		rs = await select('%s where `%s`=?' % (cls.__select__, cls.__primary_key__), [pk], 1)
		if len(rs) == 0:
			return None
		return cls(**rs[0])
	
	@asyncio.coroutine
	def save(self):
		args = list(map(self.getValueOrDefault, self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = await execute(self.__insert__, args)
		if rows != 1:
			logging.warn('failed to insert record: affected rows:%s'%rows)'''
			
#拦截点号运算。当对未定义的属性名称和实例进行点号运算时，就会用属性名作为字符串调用这个方法。如果继承树可以找到该属性，则不调用此方法
	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'model' object has no attribute '%s'"%key)
	
	def __setattr__(self, key, value):
		self[key] = value

	def getValue(self, key):
		return getattr(self, key, None)

	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s:%s'%(key,str(value)))
				setattr(self, key, value)
		return value

class Field(object):
	def  __init__(self, name, column_type, primary_key, default):
		self.name = name
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default

	def __str__(self):
		return '<%s, %s:%s>'%(self.__class__.__name__, self.column_type, self.name)

class StringField(Field):
	def __init__(self, name = None, primary_key = False, default = None, ddl = 'varchar(100)'):
		super().__init__(name, ddl, primary_key, default)
			
class ModelMetaclass(type):
	
	def __new__(cls, name, base, attrs):
		#
		if name == 'Model':
			return type.__new__(cls, name, bases, attrs)
		#
		tableName = attrs.get('__table__', None)or name
		logging.info('found model:%s (table:%s)'%(name, tableName))
		#
		mappings = dict()
		fields = []
		primaryKey = None
		for k,v in attrs.items():
			if isinstance(v, Field):
				logging.info('found mapping:%s ==> %s'%(k, v))
				mappings[k] = v
				if v.primary_key:
					#
					if primaryKey:
						raise RuntimeError('Duplicate primary key for field: %s'%k)
					primaryKey = k
					else:
						fields.append(k)
						
		if not primaryKey:
			raise RuntimeError('primary key not found.')
		for k in mappings.keys():
			attrs.pop(k)
		escaped_fields = list(map(lambda f: '`%s`' % f, fields))
		attrs['__mappings__'] = mappings # 保存属性和列的映射关系
		attrs['__table__'] = tableName
		attrs['__primary_key__'] = primaryKey # 主键属性名
		attrs['__fields__'] = fields # 除主键外的属性名
		# 构造默认的SELECT, INSERT, UPDATE和DELETE语句:
		attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ', '.join(escaped_fields), tableName)
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
		attrs['__update__'] = 'update `%s` set %s where `%s`=?' % (tableName, ', '.join(map(lambda f: '`%s`=?' % (mappings.get(f).name or f), fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s`=?' % (tableName, primaryKey)
		return type.__new__(cls, name, bases, attrs)


if __name__ = '__main__':
	user = User(id = 123, name = 'Jack')
	#
	user.insert()
	#
	users = User.findAll()
	loop = asyncio.get_event_loop()
	loop.run_until_complete(init(loop))
	loop.run_forever()