from pathlib import Path
import json


def _get_gi_name_hash(video_name: str):
	if video_name in ('MDAQ001_OPNew_Part1', 'MDAQ001_OPNew_Part2_PlayerBoy', 'MDAQ001_OPNew_Part2_PlayerGirl'):
		video_name = 'MDAQ001_OP'
	name_hash = 0
	for i in video_name:
		name_hash = (name_hash * 3 + ord(i)) & 0xffffffffffffffff
	return name_hash & 0xffffffffffffff or 0x100000000000000


def _get_gi_decrypt_key(video_name: str, version_key: int):
	return (_get_gi_name_hash(video_name) + version_key) & 0xffffffffffffff or 0x100000000000000


def get_keys():
	"""
	versions.json comes from https://github.com/HaitangYunchi/GI-cutscenes (temporary compromise)
	"""
	with open(Path(__file__).parent / 'versions.json', encoding='utf-8') as f:
		data = json.load(f)['list']
	all_keys = {}
	for version in data:
		keys = {}
		for group in version.get('videoGroups') or (version,):
			for name in group.get('videos') or ():
				if 'aesKey' in group:
					# TODO: AES keys since 7.1 are taken from versions.json until their official source is known
					# [AES video key, HCA audio key]
					keys[name] = [group['aesKey'].lower(), group['key']]
				else:
					keys[name] = _get_gi_decrypt_key(name, group.get('key') or 0)
		if keys:
			all_keys[version['version']] = keys
	return 2, all_keys


if __name__ == '__main__':
	result, all_keys = get_keys()
	keys_path = Path(__file__).parent.parent.parent / 'PyCriUsm' / 'keys.json'
	with open(keys_path, 'r', encoding='utf-8') as f:
		keys = json.load(f)
	keys["Genshin"] = {'Encrytion': result, 'KeyMap': all_keys}
	with open(keys_path, 'w', encoding='utf-8') as f:
		json.dump(keys, f, ensure_ascii=False, indent=4)