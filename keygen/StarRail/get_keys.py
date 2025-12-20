from os import write

from dataclasses_json import dataclass_json, LetterCase
from dataclasses import dataclass
import json
from pathlib import Path


@dataclass_json(letter_case=LetterCase.PASCAL)
@dataclass
class VideoConfigRow:
	VideoID: int = 0
	VideoPath: str = ''
	IsPlayerInvolved: bool = False
	CaptionPath: str = ''

@dataclass_json(letter_case=LetterCase.PASCAL)
@dataclass
class VideoEncryptionConfigRow:
	VideoID: int = 0
	Encryption: bool = False


def _get_hsr_decrypt_key_0_7(video_name: str, version_key: int):
	video_name = video_name.encode()
	name_hash = 0
	for i in video_name:
		name_hash = (name_hash * 11 + i) & 0xffffffffffffffff
	return ((version_key + name_hash) & 0xffffffffffffffff) % 72043514036987937

def _get_hsr_decrypt_key_2_2(video_name: str, version_key: int, game_version: int):
	rot = (game_version - len(video_name)) % len(video_name)
	video_name = video_name.encode()
	video_name = video_name[rot:] + video_name[:rot]
	name_hash = 0
	for i in video_name:
		name_hash = (name_hash * 7 + i) & 0xffffffffffffffff
	return ((version_key + name_hash - game_version * 10000000000000000) & 0xffffffffffffffff) % 72043514036987937

def _get_hsr_decrypt_key(video_name: str, version_key: int, game_version: int):
	if version_key < 72043514036987937:
		return _get_hsr_decrypt_key_0_7(video_name, version_key)
	else:
		return _get_hsr_decrypt_key_2_2(video_name, version_key, game_version)

def get_keys(update_for: str):
	root = Path(__file__).parent
	loaded_key = set()
	keys_path = Path(__file__).parent.parent.parent / 'PyCriUsm' / 'keys.json'
	if not keys_path.exists():
		print(f'keys.json not found at {keys_path}')
		return
	with open(keys_path, 'r', encoding='utf-8') as f:
		keys_data = json.load(f)
	all_keys = keys_data["StarRail"]["KeyMap"]
	if update_for not in all_keys:
		all_keys[update_for] = {}

	sub_root = root / update_for
	if sub_root.is_file() or sub_root.name[0].isdigit() is False:
		return
	with open(sub_root / 'GetVideoVersionKeyScRsp.json') as f:
		data = json.load(f)
	tmp_keys = {}
	for group_key in data:
		group = data[group_key]
		if isinstance(group, list):
			for item in group:
				try:
					k1, k2 = map(int, item.values())
				except Exception:
					continue
				if k1 > 10_000:
					if k2 not in tmp_keys:
						tmp_keys[k2] = k1
				else:
					if k1 not in tmp_keys:
						tmp_keys[k1] = k2

	with open(root / 'keys_from.json') as f:
		keys_from_data = json.load(f)
	video_versions = {}
	for group_key in keys_from_data:
		group = keys_from_data[group_key]
		for video in group:
			video_versions[video] = int(float(group_key) * 10)
	if update_for not in keys_from_data:
		keys_from_data[update_for] = []

	keys = {}
	with open(sub_root / 'VideoConfig.json') as f:
		data = json.load(f)
	with open(sub_root / 'LoopCGConfig.json') as f:
		data.extend(json.load(f))
	with open(sub_root / 'VideoEncryptionConfig.json') as f:
		is_encryption_data = json.load(f)
	with open(sub_root / 'LoopCGEncryptionConfig.json') as f:
		is_encryption_data.extend(json.load(f))
	if isinstance(data, dict):
		data = data.values()
	if isinstance(is_encryption_data, list):
		new_is_encryption_data = {}
		for j in is_encryption_data:
			j = VideoEncryptionConfigRow.from_dict(j)
			new_is_encryption_data[str(j.VideoID)] = j
		is_encryption_data = new_is_encryption_data
	for i in data:
		i = VideoConfigRow.from_dict(i)
		if i.VideoID in loaded_key:
			continue
		loaded_key.add(i.VideoID)
		if is_encryption_data[str(i.VideoID)].Encryption is False:
			continue
		version_key = tmp_keys.get(i.VideoID, None)
		if version_key is None:
			Warning(f'Could not find {i.VideoPath} key')
			continue
		names = i.VideoPath.removesuffix('.usm')
		if i.IsPlayerInvolved:
			names = (names + '_f', names + '_m')
		else:
			names = (names,)
		for name in names:
			if name in keys:
				continue
			if name not in video_versions:
				keys_from_data[update_for].append(name)
				video_versions[name] = int(float(update_for) * 10)
			key = _get_hsr_decrypt_key(name, version_key, video_versions[name])
			keys[name] = key

	for name in keys:
		key = keys[name]
		updated = False
		for group_key, group in all_keys.items():
			if name in group:
				group[name] = key
				updated = True
		if not updated:
			all_keys[update_for][name] = key
	return 1, all_keys, keys_from_data


if __name__ == '__main__':
	VERSION = "3.8"
	result, all_keys, keys_from_data = get_keys(VERSION)
	keys_path = Path(__file__).parent.parent.parent / 'PyCriUsm' / 'keys.json'
	keys = {}
	keys["StarRail"] = {'Encrytion': result, 'KeyMap': all_keys}
	with open(keys_path, 'w', encoding='utf-8') as f:
		json.dump(keys, f, ensure_ascii=False, indent=4)
	with open(Path(__file__).parent / 'keys_from.json', 'w', encoding='utf-8') as f:
		json.dump(keys_from_data, f, ensure_ascii=False, indent=4)