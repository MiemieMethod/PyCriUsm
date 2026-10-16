from pathlib import Path
import json


def _read_table(path: Path):
	with open(path, encoding='utf-8') as f:
		data = json.load(f)
	rows = []
	for row in next(iter(data.values())):
		video_path = key = None
		for value in row.values():
			if isinstance(value, str):
				video_path = value
			elif isinstance(value, int) and not isinstance(value, bool) and value > 0xffffffff:
				key = value
		if video_path:
			rows.append((video_path, key))
	return rows


def get_keys(update_for: str):
	keys_path = Path(__file__).parent.parent.parent / 'PyCriUsm' / 'keys.json'
	with open(keys_path, 'r', encoding='utf-8') as f:
		keys_data = json.load(f)
	all_keys = keys_data.get("ZenlessZoneZero", {}).get("KeyMap", {})
	all_keys.setdefault(update_for, {})

	sub_root = Path(__file__).parent / update_for
	for video_path, key in _read_table(sub_root / 'VideoUSMEncTemplateTb.json'):
		if not key:
			# TODO: some videos (like Gacha/Gacha_Claret) are encrypted but their keys are 0 here,
			#  the real keys may come from other data, e.g. network packets
			continue
		# only the low 7 bytes are used by USM
		key &= 0xffffffffffffff
		# keep the folders, videos in different folders may have the same name
		name = video_path.removesuffix('.usm')
		updated = False
		for group in all_keys.values():
			if name in group:
				group[name] = key
				updated = True
		if not updated:
			all_keys[update_for][name] = key
	if not all_keys[update_for]:
		del all_keys[update_for]
	return 1, all_keys


if __name__ == '__main__':
	VERSION = "3.2"
	result, all_keys = get_keys(VERSION)
	keys_path = Path(__file__).parent.parent.parent / 'PyCriUsm' / 'keys.json'
	with open(keys_path, 'r', encoding='utf-8') as f:
		keys = json.load(f)
	keys["ZenlessZoneZero"] = {'Encrytion': result, 'KeyMap': all_keys}
	with open(keys_path, 'w', encoding='utf-8') as f:
		json.dump(keys, f, ensure_ascii=False, indent=4)
