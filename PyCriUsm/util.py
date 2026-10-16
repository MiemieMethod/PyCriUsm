import asyncio
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from os import cpu_count as _cpu_count
cpu_pool = ThreadPoolExecutor()
io_pool = ThreadPoolExecutor(_cpu_count() * 2)


def init_log(name: str, is_debug=False):
	import logging
	logger = logging.getLogger(name)
	logger.setLevel(logging.DEBUG if is_debug else logging.INFO)

	console_handler = logging.StreamHandler()
	console_formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)-4s: %(message)s')
	console_handler.setFormatter(console_formatter)
	logger.addHandler(console_handler)


async def async_wait(*futures: Future):
	loop = asyncio.get_running_loop()
	ret = []
	for future in futures:
		if isinstance(future, Future):
			future = await asyncio.wrap_future(future, loop=loop)
		ret.append(future)
	return ret


def coro_wait(*futures: Future):
	ret = []
	for future in futures:
		if isinstance(future, Future):
			future = future.result()
		ret.append(future)
	return ret


def reg_dict(dic: dict, key, init_factory):
	obj = dic.get(key, None)
	if obj is None:
		obj = init_factory()
		dic[key] = obj
	return obj


_utf_formats = {0: '>B', 1: '>b', 2: '>H', 3: '>h', 4: '>I', 5: '>i', 6: '>Q', 7: '>q', 8: '>f', 9: '>d', 0xA: '>I', 0xB: '>II'}


def read_utf_table(data):
	"""
	parse a cri @UTF table, return (table name, rows)
	string columns are decoded, data columns are bytes
	"""
	from struct import unpack_from, calcsize
	table = data[8:8 + unpack_from('>I', data, 4)[0]]
	rows_offset, strings_offset, data_offset, name_offset, column_num, row_size, row_num = unpack_from('>IIIIHHI', table)
	rows_offset &= 0xffff

	def read_string(offset):
		start = strings_offset + offset
		return table[start:table.index(b'\0', start)].decode('utf-8', 'replace')

	def read_value(fmt, kind, pos):
		value = unpack_from(fmt, table, pos)
		if kind == 0xA:
			return read_string(value[0])
		if kind == 0xB:
			return table[data_offset + value[0]:data_offset + value[0] + value[1]]
		return value[0]

	columns = []
	pos = 24
	row_pos = 0
	for _ in range(column_num):
		flag = table[pos]
		name = read_string(unpack_from('>I', table, pos + 1)[0])
		pos += 5
		kind, storage = flag & 0xF, flag >> 4
		fmt = _utf_formats[kind]
		if storage == 3:
			columns.append((name, read_value(fmt, kind, pos), None, None))
			pos += calcsize(fmt)
		elif storage == 5:
			columns.append((name, kind, fmt, row_pos))
			row_pos += calcsize(fmt)
		else:
			columns.append((name, None, None, None))
	rows = []
	for row in range(row_num):
		base = rows_offset + row * row_size
		rows.append({name: value if fmt is None else read_value(fmt, value, base + offset)
					 for name, value, fmt, offset in columns})
	return read_string(name_offset), rows
